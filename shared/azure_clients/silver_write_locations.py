"""
shared/azure_clients/silver_writer_locations.py

Reads from bronze.nexudus_locations, transforms, and MERGEs
into silver.nexudus_locations and silver.nexudus_location_hours.

Upsert key: source_id (Nexudus Id) — always reflects current state.
"""
import json
import logging
import uuid

from shared.azure_clients.sql_client import get_sql_client
from shared.nexudus.transformers.locations import transform_location, transform_location_hours

logger = logging.getLogger(__name__)


class SilverLocationsWriter:

    def __init__(self, sync_run_id: uuid.UUID):
        self.sync_run_id = str(sync_run_id)
        self.sql = get_sql_client()

    def run(self) -> tuple[int, int]:
        """
        Read latest bronze records, transform, upsert to silver.
        Returns (locations_upserted, hours_rows_upserted).
        """
        bronze_rows = self._load_latest_bronze()
        logger.info(f"Loaded {len(bronze_rows)} bronze location records")

        loc_count = 0
        hours_count = 0

        for row in bronze_rows:
            bronze_id = row["id"]
            raw = json.loads(row["raw_json"])

            try:
                loc = transform_location(raw, bronze_id, self.sync_run_id)
                if loc is None:
                    logger.debug(f"Skipping excluded location source_id={raw.get('Id')}")
                    continue
                self._upsert_location(loc)
                loc_count += 1
            except Exception as e:
                logger.warning(f"Location transform failed for bronze_id={bronze_id}: {e}")
                continue

            try:
                hours_rows = transform_location_hours(raw)
                if hours_rows is not None:
                    for h in hours_rows:
                        self._upsert_hours(h)
                    hours_count += len(hours_rows)
            except Exception as e:
                logger.warning(f"Hours transform failed for source_id={raw.get('Id')}: {e}")

        logger.info(f"Silver upserted: {loc_count} locations, {hours_count} hours rows")
        return loc_count, hours_count

    # ── Bronze loader ─────────────────────────────────────────

    def _load_latest_bronze(self) -> list[dict]:
        """
        Load the most recent snapshot per source_id.
        (In case bronze has multiple runs — we only want the latest.)
        """
        return self.sql.execute_query("""
            SELECT b.id, b.source_id, b.raw_json
            FROM bronze.nexudus_locations b
            INNER JOIN (
                SELECT source_id, MAX(synced_at) AS latest
                FROM bronze.nexudus_locations
                GROUP BY source_id
            ) latest ON b.source_id = latest.source_id
                    AND b.synced_at = latest.latest
        """)

    # ── Upserts ───────────────────────────────────────────────

    def _upsert_location(self, loc: dict):
        self.sql.execute_non_query("""
            MERGE silver.nexudus_locations AS target
            USING (SELECT ? AS source_id) AS source
                ON target.source_id = source.source_id
            WHEN MATCHED THEN UPDATE SET
                bronze_id     = ?,
                sync_run_id   = ?,
                nexudus_uuid  = ?,
                name          = ?,
                web_address   = ?,
                address       = ?,
                postal_code   = ?,
                city          = ?,
                state         = ?,
                country_name  = ?,
                country_id    = ?,
                latitude      = ?,
                longitude     = ?,
                phone         = ?,
                email         = ?,
                web_contact   = ?,
                currency_code = ?,
                description   = ?,
                short_intro   = ?,
                created_on    = ?,
                updated_on    = ?,
                last_synced_at = GETUTCDATE()
            WHEN NOT MATCHED THEN INSERT (
                source_id, bronze_id, sync_run_id,
                nexudus_uuid, name, web_address,
                address, postal_code, city, state,
                country_name, country_id, latitude, longitude,
                phone, email, web_contact, currency_code,
                description, short_intro, created_on, updated_on
            ) VALUES (
                ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?, ?
            );
        """, (
            # USING clause
            loc["source_id"],
            # UPDATE SET
            loc["bronze_id"],   loc["sync_run_id"],
            loc["nexudus_uuid"], loc["name"],       loc["web_address"],
            loc["address"],     loc["postal_code"], loc["city"],    loc["state"],
            loc["country_name"],loc["country_id"],  loc["latitude"],loc["longitude"],
            loc["phone"],       loc["email"],       loc["web_contact"],
            loc["currency_code"],
            loc["description"], loc["short_intro"],
            loc["created_on"],  loc["updated_on"],
            # INSERT VALUES
            loc["source_id"],   loc["bronze_id"],   loc["sync_run_id"],
            loc["nexudus_uuid"],loc["name"],         loc["web_address"],
            loc["address"],     loc["postal_code"],  loc["city"],    loc["state"],
            loc["country_name"],loc["country_id"],   loc["latitude"],loc["longitude"],
            loc["phone"],       loc["email"],        loc["web_contact"],
            loc["currency_code"],
            loc["description"], loc["short_intro"],
            loc["created_on"],  loc["updated_on"],
        ))

    def _upsert_hours(self, h: dict):
        self.sql.execute_non_query("""
            MERGE silver.nexudus_location_hours AS target
            USING (
                SELECT ? AS location_source_id, ? AS day_of_week
            ) AS source
                ON target.location_source_id = source.location_source_id
               AND target.day_of_week        = source.day_of_week
            WHEN MATCHED THEN UPDATE SET
                is_closed      = ?,
                open_time      = ?,
                close_time     = ?,
                last_synced_at = GETUTCDATE()
            WHEN NOT MATCHED THEN INSERT (
                location_source_id, day_of_week, day_name,
                is_closed, open_time, close_time
            ) VALUES (?, ?, ?, ?, ?, ?);
        """, (
            # USING
            h["location_source_id"], h["day_of_week"],
            # UPDATE
            h["is_closed"], h["open_time"], h["close_time"],
            # INSERT
            h["location_source_id"], h["day_of_week"], h["day_name"],
            h["is_closed"], h["open_time"], h["close_time"],
        ))
