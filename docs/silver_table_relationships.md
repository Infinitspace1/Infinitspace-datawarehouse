# Silver Layer — Table Relationships

This document describes the relationships between all silver tables sourced from Nexudus.

---

## Entity Overview

```
silver.nexudus_locations          (one per coworking space)
    │
    ├──< silver.nexudus_location_hours    (7 rows per location, one per day of week)
    │
    ├──< silver.nexudus_products          (desks, offices, meeting rooms, etc.)
    │       │
    │       ├──< silver.nexudus_contracts (one contract per member-product assignment)
    │       │
    │       └──  silver.nexudus_resources (1:1, only for products of type 4 or 5)
    │
    └──< silver.nexudus_extra_services    (pricing tiers for bookable resources)
             │
             └──  (soft link) → silver.nexudus_products via resource_type_name
```

---

## Table-by-Table Relationships

### silver.nexudus_locations
The root entity. Every other table links back to a location.

| Column | Type | Description |
|--------|------|-------------|
| `source_id` | BIGINT (PK) | Nexudus `BusinessId` — used as FK in all child tables |

---

### silver.nexudus_location_hours
One row per day of week per location (7 rows per location).

| Column | FK → | Description |
|--------|------|-------------|
| `location_source_id` | `silver.nexudus_locations.source_id` | Which location these hours belong to |
| `day_of_week` | — | 1=Monday … 7=Sunday |

**Cardinality**: 1 location → 7 location_hours rows (one per day).

---

### silver.nexudus_products
Represents every physical or virtual product at a location: private offices, dedicated desks, hot desks, meeting rooms, and other bookable items.

| Column | FK → | Description |
|--------|------|-------------|
| `location_source_id` | `silver.nexudus_locations.source_id` | Where this product lives |
| `resource_id` | `silver.nexudus_resources.source_id` (soft) | Only populated for types 4+5 |

**Item types**:

| `item_type` | `product_type_label` | Has `resource_id`? | Has amenities? |
|-------------|---------------------|-------------------|----------------|
| 1 | Private Office | No | No |
| 2 | Dedicated Desk | No | No |
| 3 | Hot Desk | No | No |
| 4 | Other (bookable) | Yes | Yes |
| 5 | Meeting Room | Yes | Yes |

**Cardinality**: 1 location → many products.

---

### silver.nexudus_contracts
Represents a membership agreement between a coworker and a location. A contract is always tied to a tariff (plan) and optionally to one or more products (desks/offices assigned to the member).

| Column | FK → | Description |
|--------|------|-------------|
| `location_source_id` | `silver.nexudus_locations.source_id` | Which location issued the contract |
| `floor_plan_desk_ids` | `silver.nexudus_products.source_id` (soft, comma-separated) | Desk(s) or office(s) assigned to this contract |

**Notes**:
- `floor_plan_desk_ids` is a comma-separated string, e.g. `"1415402817"` or `"1415402776,1415468319"`.
  To join: `STRING_SPLIT(floor_plan_desk_ids, ',')` or parse in application layer.
- A contract with no `floor_plan_desk_ids` is a floating/hotdesk membership (no fixed seat).
- Products of types 1 and 2 (offices, dedicated desks) are typically assigned via contracts.
- Products of types 3, 4, and 5 are booked ad-hoc (not via contract assignment).

**Cardinality**: 1 product → many contracts (over time). 1 contract → 1 or more products.

---

### silver.nexudus_resources
Detailed resource metadata for bookable spaces (meeting rooms, phone booths, etc.). This table enriches products of type 4 and 5 with resource-specific details.

| Column | FK → | Description |
|--------|------|-------------|
| `location_source_id` | `silver.nexudus_locations.source_id` | Which location this resource is at |
| `source_id` | `silver.nexudus_products.resource_id` (soft) | Join back to the product using this |

**Cardinality**: 1 product (type 4 or 5) ↔ 1 resource (1:1).

---

### silver.nexudus_extra_services
Pricing tiers and booking configurations for resources at each location. An extra service defines *how much* it costs to book a resource (e.g., a meeting room) for a given duration or period. Multiple pricing tiers can exist per location/resource type (e.g., member rate vs. visitor rate, hourly vs. day rate).

| Column | FK → | Description |
|--------|------|-------------|
| `location_source_id` | `silver.nexudus_locations.source_id` | Which location this pricing belongs to |
| `resource_type_names` | `silver.nexudus_products.resource_type_name` (soft, comma-separated) | Which resource types this price applies to. NULL = applies to all resources at the location. |

**`charge_period` values**:

| Value | Meaning |
|-------|---------|
| 1 | Per booking |
| 4 | Per month |
| 5 | Per day |

**Cardinality**: 1 location → many extra services. 1 extra service → 0 or many products (filtered by `resource_type_names`).

**Key flags** to filter pricing tiers:

| Flag | Meaning |
|------|---------|
| `is_default_price = 1` | The standard/default rate for that resource type |
| `only_for_members = 1` | Only bookable by active members |
| `only_for_contacts = 1` | Only bookable by registered contacts |
| `apply_charge_to_visitors = 1` | Applies to visitor bookings |

---

## Join Examples

### All products at a location
```sql
SELECT p.*
FROM silver.nexudus_products p
WHERE p.location_source_id = <location_source_id>;
```

### Active contracts for a specific product (desk/office)
```sql
SELECT c.*
FROM silver.nexudus_contracts c
WHERE c.active = 1
  AND EXISTS (
      SELECT 1 FROM STRING_SPLIT(c.floor_plan_desk_ids, ',')
      WHERE value = CAST(<product_source_id> AS NVARCHAR)
  );
```

### Meeting rooms with their pricing tiers (products + extra services)
```sql
SELECT
    p.name                  AS room_name,
    p.resource_type_name,
    p.capacity,
    es.name                 AS price_tier,
    es.price,
    es.currency_code,
    es.charge_period,
    es.only_for_members
FROM silver.nexudus_products p
JOIN silver.nexudus_extra_services es
    ON  es.location_source_id = p.location_source_id
    AND (
        es.resource_type_names IS NULL
        OR es.resource_type_names LIKE '%' + p.resource_type_name + '%'
    )
WHERE p.item_type IN (4, 5)
ORDER BY p.location_source_id, p.name, es.is_default_price DESC;
```

### Full picture: location → room → price → resource details
```sql
SELECT
    l.name                  AS location,
    p.name                  AS product_name,
    p.item_type,
    p.capacity,
    r.source_id             AS resource_id,
    es.name                 AS pricing_tier,
    es.price,
    es.currency_code
FROM silver.nexudus_locations l
JOIN silver.nexudus_products p       ON p.location_source_id = l.source_id
LEFT JOIN silver.nexudus_resources r ON r.source_id = p.resource_id
LEFT JOIN silver.nexudus_extra_services es
    ON  es.location_source_id = l.source_id
    AND (es.resource_type_names IS NULL OR es.resource_type_names LIKE '%' + p.resource_type_name + '%')
WHERE p.item_type IN (4, 5)
ORDER BY l.name, p.name;
```

---

## Relationship Diagram (ER-style)

```
nexudus_locations (1)
│   source_id  ←────────────────────────────────────────────────────────┐
│                                                                         │
├──(1:7)── nexudus_location_hours                                        │
│              location_source_id ───────────────────────────────────────┘
│
├──(1:N)── nexudus_products                                              │
│              location_source_id ───────────────────────────────────────┘
│              source_id ←──────────────────────────────────────────┐
│              resource_id ────────────────────┐                    │
│                                              │                    │
│          nexudus_resources (1:1 w/ type 4+5) │                    │
│              source_id ──────────────────────┘                    │
│              location_source_id ──────────────────────────────────┘
│
├──(1:N)── nexudus_contracts
│              location_source_id (issued by)
│              floor_plan_desk_ids ──────────── soft → nexudus_products.source_id
│
└──(1:N)── nexudus_extra_services
               location_source_id
               resource_type_names ─────────── soft → nexudus_products.resource_type_name
```

---

## Key Design Decisions

1. **Soft FKs, not hard constraints** — Nexudus data can reference IDs that don't yet exist in our snapshot (e.g., a contract's `floor_plan_desk_ids` referencing a product not yet synced). Hard FK constraints would break ingestion; soft FKs allow the data to land and be validated at query time.

2. **Comma-separated IDs** — `floor_plan_desk_ids` and `resource_type_names` follow Nexudus's own API format. Use `STRING_SPLIT()` in SQL or parse in Python to expand into rows.

3. **Extra services = pricing layer for types 4+5** — Products of type 1/2/3 (offices, desks) are priced via contracts/tariffs. Products of type 4/5 (bookable resources) are priced via extra services. These are two separate billing paths in Nexudus.

4. **Resources enrich products** — `silver.nexudus_resources` is a detail table for products that are bookable (types 4+5). It contains amenity flags, booking configurations, and resource-level metadata not available in the floorplan desk record.
