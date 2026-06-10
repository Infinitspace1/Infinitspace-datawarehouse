"""
shared/hubspot/transformers/marketing_emails.py

Transforms raw bronze.hubspot_marketing_emails JSON into a typed dict
for silver.hubspot_marketing_emails.

Pure function — no I/O. Fully flat: no JSON columns in silver (the raw
payload is always available in bronze.hubspot_marketing_emails.raw_json).

KPI mapping (from the embedded `stats` object, present when the list was
fetched with includeStats=true). Real production payload shape (verified
2026-06-10 against hub 19741777):
  stats.counters: sent, open, delivered, bounce, unsubscribed, click,
                  reply, dropped, selected, spamreport, suppressed,
                  hardbounced, softbounced, pending, contactslost, notsent
  stats.ratios:   openratio, clickratio, clickthroughratio, deliveredratio,
                  bounceratio, unsubscribedratio, replyratio,
                  spamreportratio, hardbounceratio, softbounceratio,
                  contactslostratio, pendingratio, notsentratio
                  -> PERCENTAGES (e.g. 30.901 = 30.9%), not fractions
  stats.deviceBreakdown: open_device_type / click_device_type
                  {computer, mobile, unknown}

Counters/ratios keys are read case-insensitively with fallback aliases
(HubSpot has used hardbounced/hardbounces variants across portals).

Body/content: drag-and-drop emails carry no flat HTML field; the readable
body lives in content.widgets[*].body.html (rich-text modules). body_html
concatenates those in widget order; body_plain_text prefers
content.plainTextVersion and falls back to tag-stripped body_html.
"""
from __future__ import annotations

import re
from datetime import datetime, timezone
from html import unescape
from typing import Optional


def _parse_dt(value) -> Optional[datetime]:
    """Parse ISO-8601 strings or epoch milliseconds into datetime."""
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(value / 1000.0, tz=timezone.utc)
        except (ValueError, OSError, OverflowError):
            return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None


def _bit(value) -> int:
    return 1 if value else 0


def _int(value) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _decimal(value) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _str(value) -> Optional[str]:
    if value is None:
        return None
    s = str(value).strip()
    return s or None


def _ci_get(d: dict, *keys):
    """Case-insensitive dict lookup across candidate key names."""
    if not isinstance(d, dict):
        return None
    lowered = {str(k).lower(): v for k, v in d.items()}
    for key in keys:
        if key.lower() in lowered:
            return lowered[key.lower()]
    return None


def _extract_body_html(content: dict) -> Optional[str]:
    """Concatenate the HTML of all content widgets that carry one
    (rich-text modules), in widget display order."""
    widgets = content.get("widgets") or {}
    parts: list[tuple[int, str]] = []
    for widget in widgets.values():
        if not isinstance(widget, dict):
            continue
        body = widget.get("body") or {}
        html = body.get("html") if isinstance(body, dict) else None
        if isinstance(html, str) and html.strip():
            order = widget.get("order")
            parts.append((order if isinstance(order, int) else 999, html.strip()))
    if not parts:
        return None
    parts.sort(key=lambda t: t[0])
    return "\n".join(html for _, html in parts)


def _content_widgets(content: dict) -> dict:
    widgets = content.get("widgets") or {}
    return widgets if isinstance(widgets, dict) else {}


def _content_widget_names(content: dict) -> Optional[str]:
    widgets = _content_widgets(content)
    names = []
    for key, widget in widgets.items():
        if not isinstance(widget, dict):
            continue
        names.append(_str(widget.get("name")) or _str(key))
    return "|".join(name for name in names if name) or None


def _primary_content_widget(content: dict) -> dict:
    widgets = _content_widgets(content)
    preferred = widgets.get("primary_rich_text_module")
    if isinstance(preferred, dict):
        return preferred

    candidates: list[tuple[int, dict]] = []
    for widget in widgets.values():
        if not isinstance(widget, dict):
            continue
        body = widget.get("body") or {}
        if not isinstance(body, dict) or not body.get("html"):
            continue
        order = widget.get("order")
        candidates.append((order if isinstance(order, int) else 999, widget))
    if candidates:
        candidates.sort(key=lambda t: t[0])
        return candidates[0][1]
    return {}


def _primary_content_widget_body(content: dict) -> dict:
    body = _primary_content_widget(content).get("body") or {}
    return body if isinstance(body, dict) else {}


def _html_to_text(html: Optional[str]) -> Optional[str]:
    """Best-effort plain text from HTML (tags stripped, entities decoded)."""
    if not html:
        return None
    text = re.sub(r"<[^>]+>", " ", html)
    text = unescape(text).replace("\xa0", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = "\n".join(line.strip() for line in text.splitlines())
    text = re.sub(r"\n{2,}", "\n", text).strip()
    return text or None


def _extract_preview_text(content: dict) -> Optional[str]:
    widgets = content.get("widgets") or {}
    preview = widgets.get("preview_text")
    if isinstance(preview, dict):
        body = preview.get("body") or {}
        if isinstance(body, dict):
            return _str(body.get("value"))
    return None


def transform_marketing_email(raw: dict, bronze_id: int, sync_run_id: str) -> dict:
    """Transform one raw HubSpot marketing email record into a silver row dict."""
    content = raw.get("content") or {}
    from_obj = raw.get("from") or {}
    stats = raw.get("stats") or {}
    counters = stats.get("counters") or {}
    ratios = stats.get("ratios") or {}
    devices = stats.get("deviceBreakdown") or {}
    open_devices = devices.get("open_device_type") or {}
    click_devices = devices.get("click_device_type") or {}
    webversion = raw.get("webversion") or {}
    widgets = _content_widgets(content)
    primary_widget = _primary_content_widget(content)
    primary_body = _primary_content_widget_body(content)

    body_html = _extract_body_html(content)
    body_plain = _str(_ci_get(content, "plainTextVersion", "plain_text_version")) \
        or _html_to_text(body_html)

    return {
        # Source
        "source_id":            _str(raw.get("id")),
        "bronze_id":            bronze_id,
        "sync_run_id":          sync_run_id,

        # Identity
        "name":                 _str(raw.get("name")) or "",
        "subject":              _str(raw.get("subject")),
        "state":                _str(raw.get("state")),
        "email_type":           _str(raw.get("type")),
        "language":             _str(raw.get("language")),
        "archived":             _bit(raw.get("archived")),
        "is_published":         _bit(raw.get("isPublished")),

        # Campaign link
        "campaign_id":          _str(raw.get("campaign")),
        "campaign_name":        _str(raw.get("campaignName")),

        # Sender
        "from_name":            _str(_ci_get(from_obj, "fromName")),
        "reply_to":             _str(_ci_get(from_obj, "replyTo")),

        # Content / body (flattened from content.widgets)
        "subject_preview_text": _extract_preview_text(content),
        "body_html":            body_html,
        "body_plain_text":      body_plain,
        "template_path":        _str(content.get("templatePath")),
        "content_widget_count": len(widgets),
        "content_widget_names": _content_widget_names(content),
        "content_primary_widget_id": _str(primary_widget.get("id")),
        "content_primary_widget_name": _str(primary_widget.get("name")),
        "content_primary_widget_type": _str(primary_widget.get("type")),
        "content_primary_widget_module_id": _str(primary_widget.get("module_id")),
        "content_primary_widget_body_module_id": _str(primary_body.get("module_id")),
        "content_primary_widget_html": _str(primary_body.get("html")),
        "web_version_url":      _str(_ci_get(webversion, "url", "link")),

        # Timestamps
        "created_at":           _parse_dt(raw.get("createdAt")),
        "updated_at":           _parse_dt(raw.get("updatedAt")),
        "published_at":         _parse_dt(raw.get("publishedAt") or raw.get("publishDate")),

        # KPI counters
        "stat_sent":            _int(_ci_get(counters, "sent")),
        "stat_delivered":       _int(_ci_get(counters, "delivered")),
        "stat_opens":           _int(_ci_get(counters, "open", "opens")),
        "stat_clicks":          _int(_ci_get(counters, "click", "clicks")),
        "stat_bounces":         _int(_ci_get(counters, "bounce", "bounces")),
        "stat_unsubscribed":    _int(_ci_get(counters, "unsubscribed")),
        "stat_replies":         _int(_ci_get(counters, "reply", "replies")),
        "stat_spam_reports":    _int(_ci_get(counters, "spamreport", "spamReports")),
        "stat_dropped":         _int(_ci_get(counters, "dropped")),
        "stat_selected":        _int(_ci_get(counters, "selected")),
        "stat_pending":         _int(_ci_get(counters, "pending")),
        "stat_suppressed":      _int(_ci_get(counters, "suppressed")),
        "stat_not_sent":        _int(_ci_get(counters, "notsent", "notSent")),
        "stat_hard_bounces":    _int(_ci_get(counters, "hardbounced", "hardbounces", "hardBounces")),
        "stat_soft_bounces":    _int(_ci_get(counters, "softbounced", "softbounces", "softBounces")),
        "stat_contacts_lost":   _int(_ci_get(counters, "contactslost", "contactsLost")),

        # KPI ratios (percentages as returned by HubSpot, e.g. 30.901 = 30.9%)
        "open_rate":            _decimal(_ci_get(ratios, "openratio", "openRate")),
        "click_rate":           _decimal(_ci_get(ratios, "clickratio", "clickRate")),
        "click_through_rate":   _decimal(_ci_get(ratios, "clickthroughratio", "clickThroughRate")),
        "delivered_rate":       _decimal(_ci_get(ratios, "deliveredratio", "deliveredRate")),
        "bounce_rate":          _decimal(_ci_get(ratios, "bounceratio", "bounceRate")),
        "unsubscribed_rate":    _decimal(_ci_get(ratios, "unsubscribedratio", "unsubscribedRate")),
        "reply_rate":           _decimal(_ci_get(ratios, "replyratio", "replyRate")),
        "spam_report_rate":     _decimal(_ci_get(ratios, "spamreportratio", "spamReportRate")),
        "hard_bounce_rate":     _decimal(_ci_get(ratios, "hardbounceratio", "hardBounceRate")),
        "soft_bounce_rate":     _decimal(_ci_get(ratios, "softbounceratio", "softBounceRate")),
        "contacts_lost_rate":   _decimal(_ci_get(ratios, "contactslostratio", "contactsLostRate")),
        "pending_rate":         _decimal(_ci_get(ratios, "pendingratio", "pendingRate")),
        "not_sent_rate":        _decimal(_ci_get(ratios, "notsentratio", "notSentRate")),

        # Device breakdown (stats.deviceBreakdown)
        "opens_computer":       _int(_ci_get(open_devices, "computer")),
        "opens_mobile":         _int(_ci_get(open_devices, "mobile")),
        "opens_unknown":        _int(_ci_get(open_devices, "unknown")),
        "clicks_computer":      _int(_ci_get(click_devices, "computer")),
        "clicks_mobile":        _int(_ci_get(click_devices, "mobile")),
        "clicks_unknown":       _int(_ci_get(click_devices, "unknown")),
    }
