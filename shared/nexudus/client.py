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

import aiohttp
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)

BASE_URL = "https://spaces.nexudus.com/api"
DEFAULT_PAGE_SIZE = 100


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

    async def get_invoice_pdf(self, invoice_source_id: int) -> Optional[bytes]:
        """
        Download the PDF for a coworker invoice.

        Endpoint:
            GET /api/billing/coworkerinvoice/{id}/pdf

        Returns raw PDF bytes, or None if the invoice has no PDF (404).
        """
        url = f"{BASE_URL}/billing/coworkerinvoice/{invoice_source_id}/pdf"
        try:
            async with self._semaphore:
                async with self._session.get(url) as resp:
                    if resp.status == 404:
                        logger.warning("No PDF for invoice %s", invoice_source_id)
                        return None
                    resp.raise_for_status()
                    return await resp.read()
        except aiohttp.ClientResponseError as e:
            if e.status == 404:
                logger.warning("No PDF for invoice %s", invoice_source_id)
                return None
            raise
