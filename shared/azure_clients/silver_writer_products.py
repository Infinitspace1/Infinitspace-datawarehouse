"""
shared/azure_clients/silver_writer_products.py

Reads bronze.nexudus_products, transforms, and MERGEs into
silver.nexudus_products (single table, all columns).
"""
import json
import logging
import uuid

from shared.azure_clients.sql_client import get_sql_client
from shared.nexudus.transformers.products import transform_product

logger = logging.getLogger(__name__)


class SilverProductsWriter:

    def __init__(self, sync_run_id: uuid.UUID):
        self.sync_run_id = str(sync_run_id)
        self.sql = get_sql_client()

    def run(self) -> dict[str, int]:
        rows = self._load_latest_bronze()
        logger.info(f"Loaded {len(rows)} bronze product records")

        ok = errors = 0
        for row in rows:
            raw = json.loads(row["raw_json"])
            try:
                p = transform_product(raw, row["id"], self.sync_run_id)
                if p["location_source_id"] in {1376491116, 1376491117}:
                    continue    # skip beyond Global products
                self._upsert(p)
                ok += 1
            except Exception as e:
                logger.warning(f"Failed source_id={raw.get('Id')}: {e}")
                errors += 1

        logger.info(f"Silver products: {ok} upserted, {errors} errors")
        return {"products": ok, "errors": errors}

    def _load_latest_bronze(self) -> list[dict]:
        return self.sql.execute_query("""
            SELECT b.id, b.raw_json
            FROM bronze.nexudus_products b
            INNER JOIN (
                SELECT source_id, MAX(synced_at) AS latest
                FROM bronze.nexudus_products
                GROUP BY source_id
            ) latest ON b.source_id = latest.source_id
                    AND b.synced_at  = latest.latest
        """)

    def _upsert(self, p: dict):
        cols_update = """
            bronze_id = ?, sync_run_id = ?,
            item_type = ?, product_type_label = ?,
            location_source_id = ?, location_name = ?, floor_plan_id = ?, floor_plan_name = ?,
            name = ?, area_code = ?,
            price = ?, currency_code = ?,
            is_available = ?, available_from = ?, available_to = ?,
            coworker_id = ?, coworker_name = ?, coworker_company = ?,
            coworker_email = ?, contract_ids_raw = ?,
            size_sqm = ?, custom_size_sqm = ?, capacity = ?, size_is_linked_to_area = ?,
            resource_id = ?, resource_name = ?, resource_type_name = ?,
            resource_allocation = ?, resource_shifts = ?,
            amenity_air_conditioning = ?, amenity_heating = ?, amenity_internet = ?,
            amenity_large_display = ?, amenity_natural_light = ?, amenity_whiteboard = ?,
            amenity_soundproof = ?, amenity_quiet_zone = ?, amenity_tea_coffee = ?,
            amenity_security_lock = ?, amenity_cctv = ?, amenity_catering = ?,
            amenity_conference_phone = ?, amenity_projector = ?, amenity_standing_desk = ?,
            amenity_drinks = ?, amenity_privacy_screen = ?, amenity_voice_recorder = ?,
            amenity_standard_phone = ?, amenity_wireless_charger = ?,
            created_on = ?, updated_on = ?,
            last_synced_at = GETUTCDATE()
        """

        vals = (
            p["bronze_id"],         p["sync_run_id"],
            p["item_type"],         p["product_type_label"],
            p["location_source_id"],p["location_name"],
            p["floor_plan_id"],     p["floor_plan_name"],
            p["name"],              p["area_code"],
            p["price"],             p["currency_code"],
            p["is_available"],      p["available_from"],    p["available_to"],
            p["coworker_id"],       p["coworker_name"],     p["coworker_company"],
            p["coworker_email"],    p["contract_ids_raw"],
            p["size_sqm"],          p["custom_size_sqm"],
            p["capacity"],          p["size_is_linked_to_area"],
            p["resource_id"],       p["resource_name"],     p["resource_type_name"],
            p["resource_allocation"],p["resource_shifts"],
            p["amenity_air_conditioning"], p["amenity_heating"],       p["amenity_internet"],
            p["amenity_large_display"],    p["amenity_natural_light"], p["amenity_whiteboard"],
            p["amenity_soundproof"],       p["amenity_quiet_zone"],    p["amenity_tea_coffee"],
            p["amenity_security_lock"],    p["amenity_cctv"],          p["amenity_catering"],
            p["amenity_conference_phone"], p["amenity_projector"],     p["amenity_standing_desk"],
            p["amenity_drinks"],           p["amenity_privacy_screen"],p["amenity_voice_recorder"],
            p["amenity_standard_phone"],   p["amenity_wireless_charger"],
            p["created_on"],        p["updated_on"],
        )

        self.sql.execute_non_query(f"""
            MERGE silver.nexudus_products AS target
            USING (SELECT ? AS source_id) AS source
                ON target.source_id = source.source_id
            WHEN MATCHED THEN UPDATE SET {cols_update}
            WHEN NOT MATCHED THEN INSERT (
                source_id, bronze_id, sync_run_id,
                item_type, product_type_label,
                location_source_id, location_name, floor_plan_id, floor_plan_name,
                name, area_code,
                price, currency_code,
                is_available, available_from, available_to,
                coworker_id, coworker_name, coworker_company,
                coworker_email, contract_ids_raw,
                size_sqm, custom_size_sqm, capacity, size_is_linked_to_area,
                resource_id, resource_name, resource_type_name,
                resource_allocation, resource_shifts,
                amenity_air_conditioning, amenity_heating, amenity_internet,
                amenity_large_display, amenity_natural_light, amenity_whiteboard,
                amenity_soundproof, amenity_quiet_zone, amenity_tea_coffee,
                amenity_security_lock, amenity_cctv, amenity_catering,
                amenity_conference_phone, amenity_projector, amenity_standing_desk,
                amenity_drinks, amenity_privacy_screen, amenity_voice_recorder,
                amenity_standard_phone, amenity_wireless_charger,
                created_on, updated_on
            ) VALUES (
                ?, ?, ?,
                ?, ?,
                ?, ?, ?, ?,
                ?, ?,
                ?, ?,
                ?, ?, ?,
                ?, ?, ?,
                ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?,
                ?, ?,
                ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?,
                ?, ?,
                ?, ?
            );
        """, (p["source_id"], *vals, p["source_id"], *vals))