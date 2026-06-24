"""
Unit tests for the Nexudus COWORKER_INVOICE_PRINT PDF flow in
shared/nexudus/client.py (run-command -> temporary download).

No network or credentials — the aiohttp session is faked.
"""
import unittest
from urllib.parse import quote, urlsplit

import aiohttp

from shared.nexudus.client import (
    BASE_URL,
    INVOICE_PRINT_COMMAND_KEY,
    ROOT_URL,
    NexudusClient,
)

# A representative RedirectURL like the one Nexudus support documented — note
# the unencoded spaces in the downloadFileName.
PDF_REDIRECT = (
    "/ContentDownload/DownloadTempDataFile?uniqueId=544c508a-8547-4a08-8b36-bf35f9eb8b1a"
    "&downloadFileName=Invoice INV 0041 2026 06 18   Vanessa Callan.pdf"
)
ZIP_REDIRECT = (
    "/ContentDownload/DownloadTempDataFile?uniqueId=f57b13b4-8b73-4be7-8711-2b5ec61c36c6"
    "&downloadFileName=Invoices.zip"
)
# A zip whose filename is NOT the last query param and is not literally
# "Invoices.zip" — exercises the parse-based guard, not a string heuristic.
ZIP_REDIRECT_FILENAME_NOT_LAST = (
    "/ContentDownload/DownloadTempDataFile?downloadFileName=export 2026.zip"
    "&uniqueId=f57b13b4-8b73-4be7-8711-2b5ec61c36c6"
)


class _FakeResponse:
    def __init__(self, *, status=200, json_data=None, body=b"", headers=None):
        self.status = status
        self._json = json_data
        self._body = body
        self.headers = headers or {}

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    def raise_for_status(self):
        if self.status >= 400:
            raise aiohttp.ClientResponseError(
                request_info=None, history=(), status=self.status
            )

    async def json(self):
        return self._json

    async def read(self):
        return self._body


class _FakeSession:
    """Returns queued responses (last one is reused once the queue drains)."""

    def __init__(self, *, post_responses=None, get_responses=None):
        self._post = list(post_responses or [])
        self._get = list(get_responses or [])
        self.post_calls = []
        self.get_calls = []

    @staticmethod
    def _next(queue, calls_len):
        if not queue:
            raise AssertionError("no fake response queued")
        return queue[calls_len] if calls_len < len(queue) else queue[-1]

    def post(self, url, json=None):
        resp = self._next(self._post, len(self.post_calls))
        self.post_calls.append({"url": url, "json": json})
        return resp

    def get(self, url, params=None):
        resp = self._next(self._get, len(self.get_calls))
        self.get_calls.append({"url": url, "params": params})
        return resp


def _client_with(session) -> NexudusClient:
    client = NexudusClient("dummy-token")
    client._session = session
    return client


def _ok_command(redirect=PDF_REDIRECT):
    return _FakeResponse(
        status=200,
        json_data={"Status": 200, "WasSuccessful": True, "RedirectURL": redirect},
    )


def _pdf(body=b"%PDF-1.7 real pdf content"):
    return _FakeResponse(status=200, body=body)


class TestRunInvoicePrintCommand(unittest.IsolatedAsyncioTestCase):

    async def test_request_shape_and_redirect_url(self):
        session = _FakeSession(post_responses=[_ok_command()])
        client = _client_with(session)

        redirect = await client.run_invoice_print_command([1429261774])

        self.assertEqual(redirect, PDF_REDIRECT)
        self.assertEqual(len(session.post_calls), 1)
        call = session.post_calls[0]
        self.assertEqual(call["url"], f"{BASE_URL}/billing/coworkerInvoices/runCommand")
        self.assertEqual(
            call["json"],
            {
                "Ids": [1429261774],
                "Key": INVOICE_PRINT_COMMAND_KEY,
                "Parameters": [],
            },
        )

    async def test_unsuccessful_returns_none(self):
        session = _FakeSession(
            post_responses=[
                _FakeResponse(
                    status=200,
                    json_data={"WasSuccessful": False, "Message": "nope", "RedirectURL": None},
                )
            ]
        )
        client = _client_with(session)
        self.assertIsNone(await client.run_invoice_print_command([1]))

    async def test_missing_redirect_url_returns_none(self):
        session = _FakeSession(
            post_responses=[
                _FakeResponse(status=200, json_data={"WasSuccessful": True, "RedirectURL": None})
            ]
        )
        client = _client_with(session)
        self.assertIsNone(await client.run_invoice_print_command([1]))

    async def test_404_returns_none_without_retry(self):
        session = _FakeSession(post_responses=[_FakeResponse(status=404)])
        client = _client_with(session)
        self.assertIsNone(await client.run_invoice_print_command([1]))


class TestDownloadTempFile(unittest.IsolatedAsyncioTestCase):

    async def test_builds_root_url_with_percent_encoded_query(self):
        body = b"%PDF-1.7\n...bytes..."
        session = _FakeSession(get_responses=[_pdf(body)])
        client = _client_with(session)

        result = await client.download_temp_file(PDF_REDIRECT)

        self.assertEqual(result, body)
        self.assertEqual(len(session.get_calls), 1)
        # No aiohttp `params` — the query is baked into the URL with %20.
        call = session.get_calls[0]
        self.assertIsNone(call["params"])
        parts = urlsplit(PDF_REDIRECT)
        expected = f"{ROOT_URL}{parts.path}?{quote(parts.query, safe='=&%')}"
        self.assertEqual(call["url"], expected)
        self.assertIn("%20", call["url"])          # spaces are %20, not '+'
        self.assertNotIn("+", call["url"])
        self.assertIn("uniqueId=544c508a-8547-4a08-8b36-bf35f9eb8b1a", call["url"])


class TestGetInvoicePdf(unittest.IsolatedAsyncioTestCase):

    async def test_happy_path_returns_pdf_bytes(self):
        pdf = b"%PDF-1.7 real pdf content"
        session = _FakeSession(post_responses=[_ok_command()], get_responses=[_pdf(pdf)])
        client = _client_with(session)

        result = await client.get_invoice_pdf(1429261774)

        self.assertEqual(result, pdf)
        self.assertEqual(len(session.post_calls), 1)
        self.assertEqual(len(session.get_calls), 1)

    async def test_zip_redirect_returns_none_and_skips_download(self):
        session = _FakeSession(
            post_responses=[_ok_command(redirect=ZIP_REDIRECT)],
            get_responses=[_pdf(b"PK\x03\x04zip")],
        )
        client = _client_with(session)

        self.assertIsNone(await client.get_invoice_pdf(1))
        self.assertEqual(len(session.get_calls), 0)  # never attempted the download

    async def test_zip_redirect_detected_when_filename_not_last(self):
        session = _FakeSession(
            post_responses=[_ok_command(redirect=ZIP_REDIRECT_FILENAME_NOT_LAST)],
            get_responses=[_pdf(b"PK\x03\x04zip")],
        )
        client = _client_with(session)

        self.assertIsNone(await client.get_invoice_pdf(1))
        self.assertEqual(len(session.get_calls), 0)

    async def test_remints_on_non_pdf_then_succeeds(self):
        pdf = b"%PDF-1.5 ok"
        session = _FakeSession(
            post_responses=[_ok_command()],
            get_responses=[_FakeResponse(status=200, body=b"<html>expired</html>"), _pdf(pdf)],
        )
        client = _client_with(session)

        result = await client.get_invoice_pdf(1)

        self.assertEqual(result, pdf)
        self.assertEqual(len(session.post_calls), 2)  # re-minted a fresh URL
        self.assertEqual(len(session.get_calls), 2)

    async def test_persistent_non_pdf_raises_after_attempts(self):
        session = _FakeSession(
            post_responses=[_ok_command()],
            get_responses=[_FakeResponse(status=200, body=b"<html>error</html>")],
        )
        client = _client_with(session)

        with self.assertRaises(RuntimeError):
            await client.get_invoice_pdf(1)
        self.assertEqual(len(session.get_calls), 2)  # tried twice

    async def test_download_server_error_raises_after_retries(self):
        session = _FakeSession(
            post_responses=[_ok_command()],
            get_responses=[_FakeResponse(status=500)],
        )
        client = _client_with(session)

        with self.assertRaises(aiohttp.ClientResponseError):
            await client.get_invoice_pdf(1)
        self.assertEqual(len(session.get_calls), 2)  # re-minted + retried

    async def test_no_redirect_returns_none_and_skips_download(self):
        session = _FakeSession(
            post_responses=[
                _FakeResponse(status=200, json_data={"WasSuccessful": False, "RedirectURL": None})
            ],
            get_responses=[_pdf()],
        )
        client = _client_with(session)

        self.assertIsNone(await client.get_invoice_pdf(1))
        self.assertEqual(len(session.get_calls), 0)

    async def test_pdf_with_leading_whitespace_accepted(self):
        pdf = b"\r\n%PDF-1.4 content"
        session = _FakeSession(post_responses=[_ok_command()], get_responses=[_pdf(pdf)])
        client = _client_with(session)
        self.assertEqual(await client.get_invoice_pdf(1), pdf)


if __name__ == "__main__":
    unittest.main()
