"""
functions/integrations_admin.py

Optional admin/debug HTTP routes for:
  - Xero OAuth connect/callback
  - Xero tenant listing, invoice sync, and API smoke tests

Registered only when ENABLE_ADMIN_FUNCTIONS=1.
"""
import json
import logging
import os
from typing import Any

import azure.functions as func
import requests

from shared.xero.client import XeroApiClient
from shared.xero.flow import XeroAuthFlowService
from shared.xero.invoice_sync import XeroInvoiceSyncService
from shared.xero.store import XeroStore

logger = logging.getLogger(__name__)

bp = func.Blueprint()


def _json_response(payload: dict[str, Any], status_code: int = 200) -> func.HttpResponse:
    return func.HttpResponse(
        body=json.dumps(payload, default=str),
        status_code=status_code,
        mimetype="application/json",
    )


def _parse_connection_id(req: func.HttpRequest) -> int | None:
    raw = req.params.get("connection_id")
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


@bp.route(
    route="integrations/xero/connect",
    methods=["GET"],
    auth_level=func.AuthLevel.ADMIN,
)
async def xero_connect(req: func.HttpRequest) -> func.HttpResponse:
    owner_type = req.params.get("owner_type", "workspace")
    owner_id = req.params.get("owner_id", "default")
    no_redirect = req.params.get("no_redirect") == "1"

    flow = XeroAuthFlowService()
    result = flow.start_auth(owner_type=owner_type, owner_id=owner_id)

    if no_redirect:
        return _json_response(
            {
                "status": "ok",
                "owner_type": owner_type,
                "owner_id": owner_id,
                "authorization_url": result["authorization_url"],
            }
        )

    return func.HttpResponse(
        status_code=302,
        headers={"Location": result["authorization_url"]},
    )


@bp.route(
    route="integrations/xero/callback",
    methods=["GET"],
    auth_level=func.AuthLevel.ADMIN,
)
async def xero_callback(req: func.HttpRequest) -> func.HttpResponse:
    code = req.params.get("code")
    state = req.params.get("state")
    if not code or not state:
        return _json_response(
            {"error": "Missing required query params: code and state"},
            status_code=400,
        )

    flow = XeroAuthFlowService()
    try:
        result = flow.handle_callback(code=code, state=state)
    except ValueError as exc:
        return _json_response({"error": str(exc)}, status_code=400)
    except requests.HTTPError as exc:
        logger.exception("Xero callback token exchange failed")
        status = 502
        detail = exc.response.text if exc.response is not None else str(exc)
        return _json_response({"error": "Xero token exchange failed", "detail": detail}, status_code=status)

    redirect_uri = os.getenv("XERO_POST_AUTH_REDIRECT_URI")
    if redirect_uri:
        separator = "&" if "?" in redirect_uri else "?"
        location = (
            f"{redirect_uri}{separator}status=success"
            f"&xero_connection_id={result['xero_connection_id']}"
            f"&tenant_count={result['tenant_count']}"
        )
        return func.HttpResponse(status_code=302, headers={"Location": location})

    return _json_response({"status": "ok", "result": result})


@bp.route(
    route="integrations/xero/tenants",
    methods=["GET"],
    auth_level=func.AuthLevel.ADMIN,
)
async def xero_list_tenants(req: func.HttpRequest) -> func.HttpResponse:
    owner_type = req.params.get("owner_type", "workspace")
    owner_id = req.params.get("owner_id", "default")
    connection_id_raw = req.params.get("connection_id")

    store = XeroStore()
    if connection_id_raw:
        try:
            connection_id = int(connection_id_raw)
        except ValueError:
            return _json_response({"error": "connection_id must be an integer"}, status_code=400)
        tenants = store.list_tenants(connection_id=connection_id)
    else:
        tenants = store.list_tenants(owner_type=owner_type, owner_id=owner_id)

    return _json_response(
        {
            "status": "ok",
            "tenant_count": len(tenants),
            "tenants": tenants,
        }
    )


@bp.route(
    route="integrations/xero/connections",
    methods=["GET"],
    auth_level=func.AuthLevel.ADMIN,
)
async def xero_live_connections(req: func.HttpRequest) -> func.HttpResponse:
    owner_type = req.params.get("owner_type", "workspace")
    owner_id = req.params.get("owner_id", "default")

    client = XeroApiClient(
        owner_type=owner_type,
        owner_id=owner_id,
    )
    connections = client.get_connections()

    return _json_response(
        {
            "status": "ok",
            "connection_count": len(connections),
            "connections": connections,
        }
    )


@bp.route(
    route="integrations/xero/test-contacts",
    methods=["GET"],
    auth_level=func.AuthLevel.ADMIN,
)
async def xero_test_contacts(req: func.HttpRequest) -> func.HttpResponse:
    owner_type = req.params.get("owner_type", "workspace")
    owner_id = req.params.get("owner_id", "default")
    tenant_id = req.params.get("tenant_id")
    summary_only = req.params.get("summary_only", "1") != "0"
    raw = req.params.get("raw") == "1"

    client = XeroApiClient(
        owner_type=owner_type,
        owner_id=owner_id,
        tenant_id=tenant_id,
    )
    contacts = client.get_contacts(summary_only=summary_only)

    if raw:
        return _json_response({"status": "ok", "contacts": contacts})

    contact_list = contacts.get("Contacts", []) if isinstance(contacts, dict) else []
    return _json_response(
        {
            "status": "ok",
            "summary_only": summary_only,
            "contact_count": len(contact_list),
        }
    )


@bp.route(
    route="integrations/xero/invoice-debug",
    methods=["GET"],
    auth_level=func.AuthLevel.ADMIN,
)
async def xero_invoice_debug(req: func.HttpRequest) -> func.HttpResponse:
    owner_type = req.params.get("owner_type", "workspace")
    owner_id = req.params.get("owner_id", "default")
    connection_id = _parse_connection_id(req)
    tenant_id = req.params.get("tenant_id")
    invoice_id = req.params.get("invoice_id")

    if not invoice_id:
        return _json_response({"error": "Provide ?invoice_id=<xero_invoice_id>"}, status_code=400)

    client = XeroApiClient(
        owner_type=owner_type,
        owner_id=owner_id,
        connection_id=connection_id,
        tenant_id=tenant_id,
    )
    try:
        invoice = client.get_invoice(invoice_id=invoice_id, tenant_id=tenant_id)
    except requests.HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else 502
        detail = exc.response.text if exc.response is not None else str(exc)
        return _json_response(
            {"error": "Xero invoice lookup failed", "detail": detail, "invoice_id": invoice_id},
            status_code=status,
        )

    return _json_response({"status": "ok", "invoice_id": invoice_id, "invoice": invoice})


@bp.route(
    route="integrations/xero/sync-invoices",
    methods=["GET", "POST"],
    auth_level=func.AuthLevel.ADMIN,
)
async def xero_sync_invoices(req: func.HttpRequest) -> func.HttpResponse:
    owner_type = req.params.get("owner_type", "workspace")
    owner_id = req.params.get("owner_id", "default")
    connection_id = _parse_connection_id(req)
    tenant_id = req.params.get("tenant_id")
    include_pdfs = req.params.get("include_pdfs", "0") == "1"
    force_full = req.params.get("force_full", "0") == "1"

    service = XeroInvoiceSyncService()
    stats = service.sync_invoices(
        owner_type=owner_type,
        owner_id=owner_id,
        connection_id=connection_id,
        tenant_id=tenant_id,
        include_pdfs=include_pdfs,
        force_full=force_full,
    )
    return _json_response(
        {
            "status": "ok",
            "include_pdfs": include_pdfs,
            "force_full": force_full,
            "stats": stats,
        }
    )


@bp.route(
    route="integrations/xero/invoices",
    methods=["GET"],
    auth_level=func.AuthLevel.ADMIN,
)
async def xero_list_invoices(req: func.HttpRequest) -> func.HttpResponse:
    owner_type = req.params.get("owner_type", "workspace")
    owner_id = req.params.get("owner_id", "default")
    connection_id = _parse_connection_id(req)
    tenant_id = req.params.get("tenant_id")
    try:
        top = int(req.params.get("top", "100"))
    except ValueError:
        return _json_response({"error": "top must be an integer"}, status_code=400)

    service = XeroInvoiceSyncService()
    invoices = service.list_invoices(
        owner_type=owner_type,
        owner_id=owner_id,
        connection_id=connection_id,
        tenant_id=tenant_id,
        top=top,
    )
    return _json_response(
        {
            "status": "ok",
            "invoice_count": len(invoices),
            "invoices": invoices,
        }
    )


@bp.route(
    route="integrations/xero/invoice-pdf",
    methods=["GET"],
    auth_level=func.AuthLevel.ADMIN,
)
async def xero_invoice_pdf(req: func.HttpRequest) -> func.HttpResponse:
    owner_type = req.params.get("owner_type", "workspace")
    owner_id = req.params.get("owner_id", "default")
    connection_id = _parse_connection_id(req)
    tenant_id = req.params.get("tenant_id")
    invoice_id = req.params.get("invoice_id")
    cache = req.params.get("cache", "1") != "0"

    if not invoice_id:
        return _json_response({"error": "Provide ?invoice_id=<xero_invoice_id>"}, status_code=400)

    service = XeroInvoiceSyncService()
    if tenant_id and not cache:
        cached = service.get_cached_invoice_pdf(invoice_id=invoice_id, tenant_id=tenant_id)
        if cached:
            return func.HttpResponse(
                body=cached["pdf_bytes"],
                status_code=200,
                mimetype=cached.get("content_type") or "application/pdf",
                headers={
                    "Content-Disposition": f'inline; filename="{cached.get("file_name") or f"{invoice_id}.pdf"}"',
                },
            )
        return _json_response(
            {"error": "PDF is not cached for this tenant/invoice. Retry with cache=1."},
            status_code=404,
        )

    try:
        result = service.cache_invoice_pdf(
            invoice_id=invoice_id,
            owner_type=owner_type,
            owner_id=owner_id,
            connection_id=connection_id,
            tenant_id=tenant_id,
        )
    except requests.HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else 502
        detail = exc.response.text if exc.response is not None else str(exc)
        return _json_response(
            {
                "error": "Xero invoice PDF fetch failed",
                "invoice_id": invoice_id,
                "tenant_id": tenant_id,
                "detail": detail,
                "hint": "Verify the invoice exists with /api/integrations/xero/invoice-debug and note that Xero may not return a PDF for every invoice.",
            },
            status_code=status,
        )
    cached = service.get_cached_invoice_pdf(
        invoice_id=invoice_id,
        tenant_id=result["tenant_id"],
    )
    if cached is None:
        return _json_response({"error": "Invoice PDF was fetched but not cached"}, status_code=500)

    return func.HttpResponse(
        body=cached["pdf_bytes"],
        status_code=200,
        mimetype=result["content_type"] or "application/pdf",
        headers={
            "Content-Disposition": f'inline; filename="{result["file_name"] or f"{invoice_id}.pdf"}"',
        },
    )
