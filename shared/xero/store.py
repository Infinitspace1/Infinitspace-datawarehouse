"""
shared/xero/store.py

SQL persistence for Xero OAuth state, tokens, and tenants.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from shared.xero.oauth import TokenSet
from shared.xero.token_cipher import TokenCipher


@dataclass
class StoredXeroConnection:
    id: int
    owner_type: Optional[str]
    owner_id: Optional[str]
    xero_user_id: Optional[str]
    access_token: str
    refresh_token: str
    id_token: Optional[str]
    scope: Optional[str]
    token_type: Optional[str]
    expires_at: datetime
    selected_xero_tenant_id: Optional[str]
    is_connected: bool
    last_error: Optional[str]


class XeroStore:
    def __init__(self, sql_client=None, token_cipher: Optional[TokenCipher] = None):
        if sql_client is None:
            from shared.azure_clients.sql_client import get_sql_client

            sql_client = get_sql_client()
        self.sql = sql_client
        self.cipher = token_cipher or TokenCipher.from_env()

    def create_state(
        self,
        state: str,
        owner_type: Optional[str],
        owner_id: Optional[str],
        ttl_seconds: int = 600,
    ) -> None:
        expires_at = datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)
        self.sql.execute_non_query(
            """
            INSERT INTO meta.xero_oauth_states (state, owner_type, owner_id, expires_at)
            VALUES (?, ?, ?, ?)
            """,
            (state, owner_type, owner_id, expires_at),
        )

    def consume_state(self, state: str) -> Optional[dict[str, Optional[str]]]:
        with self.sql.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT TOP 1 owner_type, owner_id
                FROM meta.xero_oauth_states
                WHERE state = ?
                  AND consumed_at IS NULL
                  AND expires_at >= GETUTCDATE()
                """,
                (state,),
            )
            row = cursor.fetchone()
            if not row:
                return None

            cursor.execute(
                """
                UPDATE meta.xero_oauth_states
                SET consumed_at = GETUTCDATE()
                WHERE state = ?
                """,
                (state,),
            )
            return {
                "owner_type": row[0],
                "owner_id": row[1],
            }

    def upsert_connection(
        self,
        owner_type: Optional[str],
        owner_id: Optional[str],
        token_set: TokenSet,
        selected_tenant_id: Optional[str] = None,
    ) -> int:
        access_token_encrypted = self.cipher.encrypt(token_set.access_token)
        refresh_token_encrypted = self.cipher.encrypt(token_set.refresh_token)
        id_token_encrypted = self.cipher.encrypt(token_set.id_token) if token_set.id_token else None
        raw_payload = json.dumps(token_set.raw_payload, default=str, ensure_ascii=False)

        with self.sql.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                MERGE meta.xero_connections AS target
                USING (SELECT ? AS owner_type, ? AS owner_id) AS source
                    ON target.owner_type = source.owner_type
                   AND target.owner_id = source.owner_id
                WHEN MATCHED THEN UPDATE SET
                    xero_user_id = ?,
                    access_token_encrypted = ?,
                    refresh_token_encrypted = ?,
                    id_token_encrypted = ?,
                    scope = ?,
                    token_type = ?,
                    expires_at = ?,
                    selected_xero_tenant_id = COALESCE(?, target.selected_xero_tenant_id),
                    is_connected = 1,
                    last_error = NULL,
                    raw_token_payload = ?,
                    updated_at = GETUTCDATE()
                WHEN NOT MATCHED THEN INSERT (
                    owner_type,
                    owner_id,
                    xero_user_id,
                    access_token_encrypted,
                    refresh_token_encrypted,
                    id_token_encrypted,
                    scope,
                    token_type,
                    expires_at,
                    selected_xero_tenant_id,
                    raw_token_payload
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
                OUTPUT inserted.id;
                """,
                (
                    owner_type,
                    owner_id,
                    token_set.xero_user_id,
                    access_token_encrypted,
                    refresh_token_encrypted,
                    id_token_encrypted,
                    token_set.scope,
                    token_set.token_type,
                    token_set.expires_at,
                    selected_tenant_id,
                    raw_payload,
                    owner_type,
                    owner_id,
                    token_set.xero_user_id,
                    access_token_encrypted,
                    refresh_token_encrypted,
                    id_token_encrypted,
                    token_set.scope,
                    token_set.token_type,
                    token_set.expires_at,
                    selected_tenant_id,
                    raw_payload,
                ),
            )
            row = cursor.fetchone()
            return int(row[0])

    def update_connection_tokens(self, connection_id: int, token_set: TokenSet) -> None:
        self.sql.execute_non_query(
            """
            UPDATE meta.xero_connections
            SET xero_user_id = ?,
                access_token_encrypted = ?,
                refresh_token_encrypted = ?,
                id_token_encrypted = ?,
                scope = ?,
                token_type = ?,
                expires_at = ?,
                is_connected = 1,
                last_error = NULL,
                raw_token_payload = ?,
                updated_at = GETUTCDATE()
            WHERE id = ?
            """,
            (
                token_set.xero_user_id,
                self.cipher.encrypt(token_set.access_token),
                self.cipher.encrypt(token_set.refresh_token),
                self.cipher.encrypt(token_set.id_token) if token_set.id_token else None,
                token_set.scope,
                token_set.token_type,
                token_set.expires_at,
                json.dumps(token_set.raw_payload, default=str, ensure_ascii=False),
                connection_id,
            ),
        )

    def mark_disconnected(self, connection_id: int, error: str) -> None:
        self.sql.execute_non_query(
            """
            UPDATE meta.xero_connections
            SET is_connected = 0,
                last_error = ?,
                updated_at = GETUTCDATE()
            WHERE id = ?
            """,
            (error[:1024], connection_id),
        )

    def save_tenants(self, connection_id: int, tenants: list[dict[str, Any]]) -> None:
        with self.sql.get_connection() as conn:
            cursor = conn.cursor()
            seen_tenant_ids: set[str] = set()

            for tenant in tenants:
                tenant_id = str(tenant.get("tenantId") or tenant.get("tenant_id") or "").strip()
                if not tenant_id:
                    continue
                seen_tenant_ids.add(tenant_id)
                cursor.execute(
                    """
                    MERGE meta.xero_tenants AS target
                    USING (SELECT ? AS xero_connection_id, ? AS xero_tenant_id) AS source
                        ON target.xero_connection_id = source.xero_connection_id
                       AND target.xero_tenant_id = source.xero_tenant_id
                    WHEN MATCHED THEN UPDATE SET
                        xero_tenant_type = ?,
                        tenant_name = ?,
                        connection_id = ?,
                        auth_event_id = ?,
                        raw_payload = ?,
                        updated_at = GETUTCDATE()
                    WHEN NOT MATCHED THEN INSERT (
                        xero_connection_id,
                        xero_tenant_id,
                        xero_tenant_type,
                        tenant_name,
                        connection_id,
                        auth_event_id,
                        raw_payload
                    ) VALUES (?, ?, ?, ?, ?, ?, ?);
                    """,
                    (
                        connection_id,
                        tenant_id,
                        tenant.get("tenantType"),
                        tenant.get("tenantName"),
                        tenant.get("id"),
                        tenant.get("authEventId"),
                        json.dumps(tenant, default=str, ensure_ascii=False),
                        connection_id,
                        tenant_id,
                        tenant.get("tenantType"),
                        tenant.get("tenantName"),
                        tenant.get("id"),
                        tenant.get("authEventId"),
                        json.dumps(tenant, default=str, ensure_ascii=False),
                    ),
                )

            # Keep tenant list aligned with latest /connections response.
            if seen_tenant_ids:
                placeholders = ", ".join("?" for _ in seen_tenant_ids)
                params = [connection_id, *sorted(seen_tenant_ids)]
                cursor.execute(
                    f"""
                    DELETE FROM meta.xero_tenants
                    WHERE xero_connection_id = ?
                      AND xero_tenant_id NOT IN ({placeholders})
                    """,
                    params,
                )

            # If a selected tenant is missing after refresh, reset to first available.
            cursor.execute(
                """
                SELECT selected_xero_tenant_id
                FROM meta.xero_connections
                WHERE id = ?
                """,
                (connection_id,),
            )
            selected = cursor.fetchone()
            selected_id = selected[0] if selected else None
            if selected_id and selected_id not in seen_tenant_ids:
                cursor.execute(
                    """
                    UPDATE meta.xero_connections
                    SET selected_xero_tenant_id = (
                        SELECT TOP 1 xero_tenant_id
                        FROM meta.xero_tenants
                        WHERE xero_connection_id = ?
                        ORDER BY id ASC
                    ),
                    updated_at = GETUTCDATE()
                    WHERE id = ?
                    """,
                    (connection_id, connection_id),
                )

    def set_selected_tenant(self, connection_id: int, tenant_id: str) -> None:
        self.sql.execute_non_query(
            """
            UPDATE meta.xero_connections
            SET selected_xero_tenant_id = ?,
                updated_at = GETUTCDATE()
            WHERE id = ?
            """,
            (tenant_id, connection_id),
        )

    def mark_invoice_sync_started(self, connection_id: int, tenant_id: str) -> None:
        self.sql.execute_non_query(
            """
            UPDATE meta.xero_tenants
            SET last_invoice_sync_started_at = GETUTCDATE(),
                last_invoice_sync_error = NULL,
                updated_at = GETUTCDATE()
            WHERE xero_connection_id = ? AND xero_tenant_id = ?
            """,
            (connection_id, tenant_id),
        )

    def mark_invoice_sync_completed(
        self,
        connection_id: int,
        tenant_id: str,
        last_invoice_modified_utc: Optional[datetime],
    ) -> None:
        self.sql.execute_non_query(
            """
            UPDATE meta.xero_tenants
            SET last_invoice_sync_completed_at = GETUTCDATE(),
                last_invoice_modified_utc = COALESCE(?, last_invoice_modified_utc),
                last_invoice_sync_error = NULL,
                updated_at = GETUTCDATE()
            WHERE xero_connection_id = ? AND xero_tenant_id = ?
            """,
            (last_invoice_modified_utc, connection_id, tenant_id),
        )

    def mark_invoice_sync_failed(self, connection_id: int, tenant_id: str, error: str) -> None:
        self.sql.execute_non_query(
            """
            UPDATE meta.xero_tenants
            SET last_invoice_sync_completed_at = GETUTCDATE(),
                last_invoice_sync_error = ?,
                updated_at = GETUTCDATE()
            WHERE xero_connection_id = ? AND xero_tenant_id = ?
            """,
            (error[:1024], connection_id, tenant_id),
        )

    def get_tenant(
        self,
        connection_id: int,
        tenant_id: str,
    ) -> Optional[dict[str, Any]]:
        rows = self.sql.execute_query(
            """
            SELECT TOP 1
                id, xero_connection_id, xero_tenant_id, xero_tenant_type,
                tenant_name, connection_id, auth_event_id, raw_payload,
                last_invoice_sync_started_at, last_invoice_sync_completed_at,
                last_invoice_modified_utc, last_invoice_sync_error,
                created_at, updated_at
            FROM meta.xero_tenants
            WHERE xero_connection_id = ? AND xero_tenant_id = ?
            """,
            (connection_id, tenant_id),
        )
        return rows[0] if rows else None

    def list_tenants(
        self,
        owner_type: Optional[str] = None,
        owner_id: Optional[str] = None,
        connection_id: Optional[int] = None,
    ) -> list[dict[str, Any]]:
        if connection_id is not None:
            return self.sql.execute_query(
                """
                SELECT t.id, t.xero_connection_id, t.xero_tenant_id, t.xero_tenant_type,
                       t.tenant_name, t.connection_id, t.auth_event_id, t.raw_payload,
                       t.last_invoice_sync_started_at, t.last_invoice_sync_completed_at,
                       t.last_invoice_modified_utc, t.last_invoice_sync_error,
                       c.selected_xero_tenant_id
                FROM meta.xero_tenants t
                INNER JOIN meta.xero_connections c ON c.id = t.xero_connection_id
                WHERE t.xero_connection_id = ?
                ORDER BY t.tenant_name
                """,
                (connection_id,),
            )

        if owner_type is None or owner_id is None:
            raise ValueError("Provide either connection_id or both owner_type and owner_id")

        return self.sql.execute_query(
            """
            SELECT t.id, t.xero_connection_id, t.xero_tenant_id, t.xero_tenant_type,
                   t.tenant_name, t.connection_id, t.auth_event_id, t.raw_payload,
                   t.last_invoice_sync_started_at, t.last_invoice_sync_completed_at,
                   t.last_invoice_modified_utc, t.last_invoice_sync_error,
                   c.selected_xero_tenant_id
            FROM meta.xero_tenants t
            INNER JOIN meta.xero_connections c ON c.id = t.xero_connection_id
            WHERE c.owner_type = ? AND c.owner_id = ?
            ORDER BY t.tenant_name
            """,
            (owner_type, owner_id),
        )

    def get_connection(
        self,
        owner_type: Optional[str] = None,
        owner_id: Optional[str] = None,
        connection_id: Optional[int] = None,
    ) -> Optional[StoredXeroConnection]:
        if connection_id is not None:
            rows = self.sql.execute_query(
                """
                SELECT TOP 1
                    id, owner_type, owner_id, xero_user_id,
                    access_token_encrypted, refresh_token_encrypted, id_token_encrypted,
                    scope, token_type, expires_at, selected_xero_tenant_id,
                    is_connected, last_error
                FROM meta.xero_connections
                WHERE id = ?
                """,
                (connection_id,),
            )
        else:
            if owner_type is None or owner_id is None:
                raise ValueError("Provide either connection_id or both owner_type and owner_id")
            rows = self.sql.execute_query(
                """
                SELECT TOP 1
                    id, owner_type, owner_id, xero_user_id,
                    access_token_encrypted, refresh_token_encrypted, id_token_encrypted,
                    scope, token_type, expires_at, selected_xero_tenant_id,
                    is_connected, last_error
                FROM meta.xero_connections
                WHERE owner_type = ? AND owner_id = ?
                ORDER BY updated_at DESC
                """,
                (owner_type, owner_id),
            )

        if not rows:
            return None
        return self._row_to_connection(rows[0])

    def _row_to_connection(self, row: dict[str, Any]) -> StoredXeroConnection:
        expires_at = row["expires_at"]
        if isinstance(expires_at, datetime) and expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)
        return StoredXeroConnection(
            id=int(row["id"]),
            owner_type=row.get("owner_type"),
            owner_id=row.get("owner_id"),
            xero_user_id=row.get("xero_user_id"),
            access_token=self.cipher.decrypt(row["access_token_encrypted"]),
            refresh_token=self.cipher.decrypt(row["refresh_token_encrypted"]),
            id_token=self.cipher.decrypt(row["id_token_encrypted"]) if row.get("id_token_encrypted") else None,
            scope=row.get("scope"),
            token_type=row.get("token_type"),
            expires_at=expires_at,
            selected_xero_tenant_id=row.get("selected_xero_tenant_id"),
            is_connected=bool(row.get("is_connected", 0)),
            last_error=row.get("last_error"),
        )
