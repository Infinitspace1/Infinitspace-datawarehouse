"""
shared/nexudus/client.py

Low-level async Nexudus API client.
Handles pagination, rate limiting, and retries.
All methods return raw dicts — no transformation here.
"""
import asyncio
import logging
import os
from typing import AsyncGenerator, Optional
from urllib.parse import parse_qsl, quote, urlsplit

import aiohttp
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)

BASE_URL = "https://spaces.nexudus.com/api"
# The temp-file download (step 3 of the invoice-print flow) lives on the bare
# host, NOT under /api.
ROOT_URL = "https://spaces.nexudus.com"
DEFAULT_PAGE_SIZE = 100

# Nexudus renders a coworker-invoice PDF via a "run command" rather than a
# direct PDF endpoint. Flow (per Nexudus support):
#   1. POST billing/coworkerInvoices/runCommand
#        {"Ids":[id],"Key":"COWORKER_INVOICE_PRINT","Parameters":[]}
#      -> 200 with a RedirectURL pointing at a temporary download file.
#   2. GET https://spaces.nexudus.com{RedirectURL}  (expires within a few
#      minutes, so it must be downloaded immediately).
# A single Id returns a PDF; two or more Ids return a zip — we only ever pass
# one Id so we always get a PDF.
INVOICE_PRINT_COMMAND_PATH = "billing/coworkerInvoices/runCommand"
INVOICE_PRINT_COMMAND_KEY = "COWORKER_INVOICE_PRINT"
_PDF_MAGIC = b"%PDF"
# How many times to (re-)mint a fresh RedirectURL and re-download when the temp
# file comes back missing/expired (the temp file lives for only a few minutes).
INVOICE_PDF_MAX_ATTEMPTS = 2


def _redirect_filename(redirect_url: str) -> str:
    """Return the ``downloadFileName`` value from a Nexudus RedirectURL ('' if absent)."""
    for key, value in parse_qsl(urlsplit(redirect_url).query, keep_blank_values=True):
        if key.lower() == "downloadfilename":
            return value
    return ""


def _is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, aiohttp.ClientResponseError):
        return exc.status in (429, 500, 502, 503, 504)
    if isinstance(exc, (aiohttp.ServerConnectionError, asyncio.TimeoutError)):
        return True
    return False


class NexudusClient:
    """
    Thin async wrapper around the Nexudus REST API.

    Usage:
        async with NexudusClient(bearer_token) as client:
            async for page in client.paginate("sys/floorplandesks"):
                ...
    """

    def __init__(self, bearer_token: str, max_concurrent: int = 3):
        self._token = bearer_token
        self._headers = {"Authorization": f"Bearer {bearer_token}"}
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self):
        timeout = aiohttp.ClientTimeout(total=60, connect=10)
        self._session = aiohttp.ClientSession(
            timeout=timeout,
            headers=self._headers,
        )
        return self

    async def __aexit__(self, *args):
        if self._session:
            await self._session.close()

    # ── Core request ─────────────────────────────────────────

    @retry(
        wait=wait_exponential(multiplier=2, min=4, max=60),
        stop=stop_after_attempt(5),
        retry=retry_if_exception(_is_retryable),
    )
    async def get(self, path: str, params: dict = None) -> dict | list:
        url = f"{BASE_URL}/{path.lstrip('/')}"
        async with self._semaphore:
            async with self._session.get(url, params=params or {}) as resp:
                if resp.status == 429:
                    wait = int(resp.headers.get("Retry-After", 15))
                    logger.warning(f"Rate limited on {path} — waiting {wait}s")
                    await asyncio.sleep(wait)
                    resp.raise_for_status()
                resp.raise_for_status()
                return await resp.json()

    # ── Pagination ───────────────────────────────────────────

    async def paginate(
        self,
        path: str,
        extra_params: dict = None,
        page_size: int = DEFAULT_PAGE_SIZE,
    ) -> AsyncGenerator[list[dict], None]:
        """
        Yields one list of records per page.
        Stops when HasNextPage is False or Records is empty.
        """
        page = 1
        while True:
            params = {"page": page, "size": page_size, **(extra_params or {})}
            data = await self.get(path, params)

            records = data.get("Records", []) if isinstance(data, dict) else data
            if not records:
                break

            logger.debug(f"{path} — page {page}: {len(records)} records")
            yield records

            if not data.get("HasNextPage", False):
                break
            page += 1

    async def get_all(self, path: str, extra_params: dict = None) -> list[dict]:
        """Convenience: collect all pages into a single list."""
        results = []
        async for page in self.paginate(path, extra_params):
            results.extend(page)
        return results

    # ── Single-record fetch ──────────────────────────────────

    async def get_one(self, path: str) -> Optional[dict]:
        """Fetch a single record by its full path (e.g. spaces/resources/123)."""
        try:
            return await self.get(path)
        except aiohttp.ClientResponseError as e:
            if e.status == 404:
                logger.warning(f"Not found: {path}")
                return None
            raise

    async def get_coworker(self, coworker_id: int) -> Optional[dict]:
        """
        Fetch a single coworker record.

        Endpoint:
            GET /api/spaces/coworkers/{id}
        """
        return await self.get_one(f"spaces/coworkers/{coworker_id}")

    async def get_coworker_invoice_lines(self, invoice_source_id: int) -> list[dict]:
        """
        Fetch all line items for a single coworker invoice.

        Endpoint:
            GET /api/billing/coworkerinvoicelines?CoworkerInvoiceLine_CoworkerInvoice={id}
        """
        return await self.get_all(
            "billing/coworkerinvoicelines",
            extra_params={"CoworkerInvoiceLine_CoworkerInvoice": invoice_source_id},
        )

    async def get_coworker_invoice_histories(self, invoice_source_id: int) -> list[dict]:
        """
        Fetch all history entries for a single coworker invoice.

        Endpoint:
            GET /api/billing/coworkerinvoicehistories?CoworkerInvoiceHistory_CoworkerInvoice={id}
        """
        return await self.get_all(
            "billing/coworkerinvoicehistories",
            extra_params={"CoworkerInvoiceHistory_CoworkerInvoice": invoice_source_id},
        )

    @retry(
        wait=wait_exponential(multiplier=2, min=4, max=60),
        stop=stop_after_attempt(5),
        retry=retry_if_exception(_is_retryable),
    )
    async def run_invoice_print_command(
        self, invoice_source_ids: list[int]
    ) -> Optional[str]:
        """
        Trigger the COWORKER_INVOICE_PRINT command for one or more invoices.

        Endpoint:
            POST /api/billing/coworkerInvoices/runCommand
            body: {"Ids":[...],"Key":"COWORKER_INVOICE_PRINT","Parameters":[]}

        Returns the temporary ``RedirectURL`` (a path under spaces.nexudus.com,
        e.g. ``/ContentDownload/DownloadTempDataFile?uniqueId=...``), or None if
        Nexudus reported the command did not succeed / produced no document.
        """
        url = f"{BASE_URL}/{INVOICE_PRINT_COMMAND_PATH}"
        payload = {
            "Ids": list(invoice_source_ids),
            "Key": INVOICE_PRINT_COMMAND_KEY,
            "Parameters": [],
        }
        try:
            async with self._semaphore:
                async with self._session.post(url, json=payload) as resp:
                    if resp.status == 429:
                        wait = int(resp.headers.get("Retry-After", 15))
                        logger.warning(
                            "Rate limited on invoice print command — waiting %ss",
                            wait,
                        )
                        await asyncio.sleep(wait)
                        resp.raise_for_status()
                    resp.raise_for_status()
                    data = await resp.json()
        except aiohttp.ClientResponseError as e:
            if e.status == 404:
                logger.warning(
                    "Invoice print command 404 for %s", invoice_source_ids
                )
                return None
            raise

        if not isinstance(data, dict):
            logger.warning(
                "Invoice print command returned unexpected payload for %s",
                invoice_source_ids,
            )
            return None
        if not data.get("WasSuccessful", False):
            logger.warning(
                "Invoice print command unsuccessful for %s: %s",
                invoice_source_ids,
                data.get("Message") or data.get("Errors"),
            )
            return None
        redirect_url = data.get("RedirectURL")
        if not redirect_url:
            logger.warning(
                "Invoice print command returned no RedirectURL for %s",
                invoice_source_ids,
            )
            return None
        return redirect_url

    async def download_temp_file(self, redirect_url: str) -> bytes:
        """
        Download a Nexudus temporary content file from a ``RedirectURL``.

        ``redirect_url`` is the relative path returned by
        ``run_invoice_print_command`` (lives on the bare host, NOT under /api).
        The temp file expires within a few minutes, so call this immediately
        after obtaining the RedirectURL. The session-level Bearer header is
        reused for authorization.

        Deliberately NOT retried with a long backoff: the temp URL is
        short-lived, so re-hitting the same (possibly expired) link is
        pointless — ``get_invoice_pdf`` re-mints a fresh URL instead.
        """
        parts = urlsplit(redirect_url)
        # Percent-encode the query so a spaced ``downloadFileName`` is sent as
        # ``%20`` (RFC 3986). Passing the raw string to aiohttp/yarl would
        # otherwise emit form-style ``+`` for spaces; ``safe="=&%"`` keeps the
        # param separators and any already-encoded ``%XX`` sequences intact.
        query = quote(parts.query, safe="=&%") if parts.query else ""
        url = f"{ROOT_URL}{parts.path}" + (f"?{query}" if query else "")
        async with self._semaphore:
            async with self._session.get(url) as resp:
                resp.raise_for_status()
                return await resp.read()

    async def get_invoice_pdf(self, invoice_source_id: int) -> Optional[bytes]:
        """
        Download the PDF for a single coworker invoice.

        Uses Nexudus's two-step COWORKER_INVOICE_PRINT command (see module
        docstring): run the command to obtain a temporary download URL, then
        fetch the file. If the temp file comes back empty/expired (or the
        download errors transiently), the whole two-step flow is retried with a
        freshly-minted URL up to ``INVOICE_PDF_MAX_ATTEMPTS`` times.

        Returns:
          - PDF bytes on success.
          - ``None`` when Nexudus reports no printable document (command
            unsuccessful / no RedirectURL) or unexpectedly returns a zip — these
            are genuine "nothing to cache" cases, not failures.
        Raises on a download that keeps failing (transient error or never a
        PDF) so the caller records it as a failure rather than a silent skip.
        """
        last_exc: Optional[Exception] = None
        for attempt in range(1, INVOICE_PDF_MAX_ATTEMPTS + 1):
            redirect_url = await self.run_invoice_print_command([invoice_source_id])
            if not redirect_url:
                # Nexudus reported no printable document — genuine skip.
                return None

            # A single Id yields a PDF; a zip means something unexpected. Check
            # the parsed downloadFileName so param order / archive name can't
            # fool the guard.
            filename = _redirect_filename(redirect_url)
            if filename.lower().endswith(".zip"):
                logger.warning(
                    "Invoice %s print returned a zip (%s), not a PDF — skipping",
                    invoice_source_id,
                    filename or "unknown",
                )
                return None

            try:
                pdf_bytes = await self.download_temp_file(redirect_url)
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                last_exc = exc
                logger.warning(
                    "Invoice %s temp-file download failed (attempt %s/%s): %s",
                    invoice_source_id,
                    attempt,
                    INVOICE_PDF_MAX_ATTEMPTS,
                    exc,
                )
                continue  # re-mint a fresh RedirectURL and retry

            if pdf_bytes and pdf_bytes.lstrip()[:4] == _PDF_MAGIC:
                return pdf_bytes

            # Empty / non-PDF body — most likely the temp file expired and we
            # got an HTML error page. Re-mint a fresh URL and try again.
            logger.warning(
                "Invoice %s download was not a PDF (%s bytes, attempt %s/%s) — re-minting",
                invoice_source_id,
                len(pdf_bytes or b""),
                attempt,
                INVOICE_PDF_MAX_ATTEMPTS,
            )

        if last_exc is not None:
            raise last_exc
        raise RuntimeError(
            f"Invoice {invoice_source_id}: temp-file download never returned a PDF "
            f"after {INVOICE_PDF_MAX_ATTEMPTS} attempts"
        )
