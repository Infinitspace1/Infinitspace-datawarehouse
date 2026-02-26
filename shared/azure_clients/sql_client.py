"""
Azure SQL Database client for InfinitSpace AI Brain
Handles all database operations with connection pooling.

Supports three connection modes (checked in order):
1. AZURE_SQL_CONNECTION_STRING — full ODBC connection string (highest priority)
2. AZURE_SQL_SERVER + AZURE_SQL_DATABASE with Entra integrated auth (no user/password needed)
3. AZURE_SQL_SERVER + AZURE_SQL_DATABASE + AZURE_SQL_USERNAME + AZURE_SQL_PASSWORD (SQL auth)
"""
import os
import re
import time
import pyodbc
from typing import List, Dict, Any, Optional
from contextlib import contextmanager
import logging
import struct
from azure.identity import DefaultAzureCredential

from dotenv import load_dotenv

load_dotenv()  # Load environment variables from .env file

logger = logging.getLogger(__name__)


class SQLClient:
    """Low-level Azure SQL client used by the dashboard APIs."""

    def __init__(self):
        self._credential = None
        
        # Option 1: Direct connection string
        direct_conn_str = os.getenv("AZURE_SQL_CONNECTION_STRING")

        if direct_conn_str:
            self.connection_string = direct_conn_str
            logger.info("Using AZURE_SQL_CONNECTION_STRING for database connection")
            return

        # Options 2 & 3: Build from individual vars
        server = os.getenv("AZURE_SQL_SERVER")
        database = os.getenv("AZURE_SQL_DATABASE")
        driver = os.getenv("AZURE_SQL_DRIVER", "ODBC Driver 18 for SQL Server")

        if not server or not database:
            raise ValueError(
                "Missing required Azure SQL config. Provide either "
                "AZURE_SQL_CONNECTION_STRING or AZURE_SQL_SERVER + AZURE_SQL_DATABASE"
            )

        username = os.getenv("AZURE_SQL_USERNAME")
        password = os.getenv("AZURE_SQL_PASSWORD")

        timeout = int(os.getenv("AZURE_SQL_CONNECTION_TIMEOUT", "60"))
        trust_cert = os.getenv("AZURE_SQL_TRUST_SERVER_CERTIFICATE", "").strip().lower() in ("1", "true", "yes")

        if username and password:
            # Option 3: SQL auth with user/password
            self.connection_string = (
                f"DRIVER={{{driver}}};"
                f"SERVER={server};"
                f"DATABASE={database};"
                f"UID={username};"
                f"PWD={password};"
                "Encrypt=yes;"
                f"TrustServerCertificate={'yes' if trust_cert else 'no'};"
                f"Connection Timeout={timeout};"
            )
            logger.info("Using SQL authentication for database connection")
        else:
            # Option 2: Microsoft Entra integrated auth
            self.connection_string = (
                f"DRIVER={{{driver}}};"
                f"SERVER={server};"
                f"DATABASE={database};"
                "Authentication=ActiveDirectoryIntegrated;"
                "Encrypt=yes;"
                f"TrustServerCertificate={'yes' if trust_cert else 'no'};"
                f"Connection Timeout={timeout};"
            )
            logger.info("Using Microsoft Entra integrated authentication for database connection")

        self._credential = None

    def _open_connection(self) -> pyodbc.Connection:
        """Open a single pyodbc connection (no retry). Uses SQL auth or Managed Identity."""
        if "UID=" in self.connection_string or "Uid=" in self.connection_string:
            return pyodbc.connect(self.connection_string)
        if self._credential is None:
            self._credential = DefaultAzureCredential()
        token = self._credential.get_token("https://database.windows.net/.default")
        token_bytes = token.token.encode("utf-16-le")
        token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)
        return pyodbc.connect(self.connection_string, attrs_before={1256: token_struct})

    @contextmanager
    def get_connection(self, retries: int = 3, retry_delay: float = 5.0):
        """Open a connection with retry logic for transient login timeouts (HYT00).

        Serverless Azure SQL auto-pauses after inactivity — the first connection
        attempt after a pause can fail with HYT00 while the database resumes.
        Retrying after a short wait is the correct fix.
        """
        conn = None
        for attempt in range(1, retries + 1):
            try:
                conn = self._open_connection()
                break
            except pyodbc.OperationalError as e:
                sqlstate = e.args[0] if e.args else ""
                if sqlstate == "HYT00" and attempt < retries:
                    logger.warning(
                        f"SQL login timeout on attempt {attempt}/{retries} "
                        f"(DB may be resuming from auto-pause). "
                        f"Retrying in {retry_delay}s..."
                    )
                    time.sleep(retry_delay)
                else:
                    raise
        try:
            yield conn
            conn.commit()
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            if conn:
                conn.close()

    def execute_query(self, query: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
        """Execute SELECT query and return results as list of dicts."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)

            columns = [column[0] for column in cursor.description]
            results: List[Dict[str, Any]] = []
            for row in cursor.fetchall():
                results.append(dict(zip(columns, row)))
            return results

    def execute_non_query(self, query: str, params: Optional[tuple] = None) -> int:
        """Execute INSERT/UPDATE/DELETE and return affected rows."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            return cursor.rowcount

    def execute_scalar(self, query: str, params: Optional[tuple] = None) -> Any:
        """Execute query and return single value."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            result = cursor.fetchone()
            return result[0] if result else None

    def insert_and_get_id(self, query: str, params: Optional[tuple] = None) -> int:
        """Execute INSERT and return the new identity value."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            cursor.execute("SELECT @@IDENTITY")
            return cursor.fetchone()[0]


# Singleton instance for the low-level client
_sql_client: Optional[SQLClient] = None


def get_sql_client() -> SQLClient:
    """Get or create SQL client singleton (dashboard APIs use this)."""
    global _sql_client
    if _sql_client is None:
        _sql_client = SQLClient()
    return _sql_client


# ---------------------------------------------------------------------------
# Backwards-compatible Database wrapper used by Ava bot + scripts
# ---------------------------------------------------------------------------


class Database:
    """High-level DB wrapper with `fetch_one/fetch_all/execute` methods.

    This preserves the original interface expected by the Ava chatbot code,
    while internally delegating to the shared `SQLClient`.
    """

    def __init__(self, client: Optional[SQLClient] = None):
        self._client = client or get_sql_client()

    @staticmethod
    def _convert_named_params(query: str, params: Optional[Dict[str, Any]]) -> tuple[str, Optional[tuple]]:
        """
        Convert :named parameters to ? placeholders for pyodbc and
        return (converted_query, ordered_values).
        """
        if not params:
            return query, None

        ordered_values: List[Any] = []

        def repl(match: re.Match) -> str:
            name = match.group(1)
            if name not in params:
                raise ValueError(f"Missing parameter value for :{name}")
            ordered_values.append(params[name])
            return "?"

        pattern = re.compile(r":([a-zA-Z_][a-zA-Z0-9_]*)")
        converted = pattern.sub(repl, query)
        return converted, tuple(ordered_values)

    def fetch_all(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        q, p = self._convert_named_params(query, params)
        return self._client.execute_query(q, p)

    def fetch_one(self, query: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        rows = self.fetch_all(query, params)
        return rows[0] if rows else None

    def execute(self, query: str, params: Optional[Dict[str, Any]] = None) -> int:
        q, p = self._convert_named_params(query, params)
        return self._client.execute_non_query(q, p)


_db: Optional[Database] = None


def get_db() -> Database:
    """Backwards-compatible getter for Ava bot + scripts."""
    global _db
    if _db is None:
        _db = Database()
    return _db
