"""
shared/firebase/client.py

Firestore client for the TeamAndy `competence_new` sync.

Mirrors the AI-teamandy connection logic. Credentials are resolved in this order:

  1. FIREBASE_CREDENTIALS — the service-account JSON as a string (used as the
     Azure app setting; same value the TeamAndy scraping services use). We
     json.loads it into a dict for credentials.Certificate(). If it is not
     valid JSON it is treated as a file path.
  2. FIREBASE_SERVICE_ACCOUNT_KEY_FILE — either a local path to the key file OR a
     download URL (the TeamAndy backend stores a Google Drive URL here and
     downloads it before calling credentials.Certificate). We support both: a URL
     is fetched and parsed in-memory; a path is used directly.

The Firestore client is initialised once and cached.
"""
from __future__ import annotations

import json
import logging
import os

import firebase_admin
import requests
from firebase_admin import credentials, firestore

logger = logging.getLogger(__name__)

_db = None  # cached firestore.Client


def _build_credentials() -> credentials.Base:
    # 1. FIREBASE_CREDENTIALS — service-account JSON as a string (Azure app
    #    setting), like the AI-teamandy scraping services. Falls back to
    #    treating the value as a path if it is not valid JSON.
    raw = os.getenv("FIREBASE_CREDENTIALS")
    if raw:
        try:
            info = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return credentials.Certificate(raw)  # treat as a file path
        return credentials.Certificate(info)

    # 2. FIREBASE_SERVICE_ACCOUNT_KEY_FILE — a local path OR a download URL
    #    (the TeamAndy backend stores a Google Drive URL here).
    key_file = os.getenv("FIREBASE_SERVICE_ACCOUNT_KEY_FILE")
    if key_file:
        if key_file.startswith(("http://", "https://")):
            return _certificate_from_url(key_file)
        if not os.path.exists(key_file):
            raise EnvironmentError(
                f"FIREBASE_SERVICE_ACCOUNT_KEY_FILE points to a missing file: {key_file}"
            )
        return credentials.Certificate(key_file)

    raise EnvironmentError(
        "No Firebase credentials configured. Set FIREBASE_CREDENTIALS (the "
        "service-account JSON, used as the Azure app setting) or "
        "FIREBASE_SERVICE_ACCOUNT_KEY_FILE (a path to, or download URL for, the "
        "service-account key file, as used by the TeamAndy backend)."
    )


def _certificate_from_url(url: str) -> credentials.Certificate:
    """Download a service-account key JSON from a URL and build a Certificate.

    Mirrors the TeamAndy backend (requests.get on a Google Drive URL), but parses
    the JSON in-memory instead of writing it to disk.
    """
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    try:
        info = resp.json()
    except ValueError as exc:
        # Google Drive returns an HTML page instead of the file when the link
        # isn't a direct/public download (or hits the large-file scan warning).
        # Service-account keys are tiny, so a non-JSON body means a bad URL.
        raise EnvironmentError(
            "FIREBASE_SERVICE_ACCOUNT_KEY_FILE URL did not return JSON "
            f"(Content-Type={resp.headers.get('Content-Type')!r}). Ensure it is a "
            "direct, publicly-readable download of the service-account key."
        ) from exc
    return credentials.Certificate(info)


def get_firestore_client():
    """Return a cached Firestore client, initialising firebase_admin once.

    firebase_admin allows only one default app per process, so we guard the
    initialise call against re-entry (the Functions host reuses the worker
    process across invocations).
    """
    global _db
    if _db is not None:
        return _db
    if not firebase_admin._apps:
        firebase_admin.initialize_app(_build_credentials())
        logger.info("Firebase initialised for competence_new sync")
    _db = firestore.client()
    return _db
