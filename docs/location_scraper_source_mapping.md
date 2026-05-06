# Location Scraper Source Mapping

Purpose: define per-source JSON field mapping from `bronze.location_scraper_raw.payload_json`
to the globe read model (`silver.location_scraper_globe_v2`).

This document is the source of truth for mapping decisions before implementing/updating SQL views.

## Common Target Fields

- `external_id`
- `listing_url`
- `latitude`
- `longitude`
- `address`
- `postal_code`
- `district`
- `city`
- `country_code`
- `price_monthly`
- `price_per_m2`
- `surface_m2`
- `contact_name`
- `company_name`
- `phone`
- `lusha_email_1`
- `lusha_contact_1`
- `lusha_title_1`
- `lusha_confidence_1`
- `lusha_email_2`
- `lusha_contact_2`
- `lusha_title_2`
- `lusha_confidence_2`
- `lusha_email_3`
- `lusha_contact_3`
- `lusha_title_3`
- `lusha_confidence_3`
- `source`
- `run_id`
- `item_index`

## Source: immobilienscout

Observed run: `local-munich-004` (Munich).

| Target field | Primary JSON path | Fallback(s) | Notes |
|---|---|---|---|
| external_id | `$.normalized.listingId` | `$.header.id`, `$.basicInfo.id`, `$.id` | String cast. |
| listing_url | `$.normalized.url` | `$.basicInfo.url`, `$.url` | Keep full URL. |
| latitude | `$.basicInfo.address.lat` | `$.sections[3].location.lat`, `$.geo_wgs84Lat`, `$.normalized.address.latitude` | Use `TRY_CAST(float)`. |
| longitude | `$.basicInfo.address.lon` | `$.sections[3].location.lng`, `$.geo_wgs84Lon`, `$.normalized.address.longitude` | Use `TRY_CAST(float)`. |
| address | `$.basicInfo.address.line` | `$.normalized.address.formatted` | Prefer full formatted address. |
| postal_code | `$.normalized.address.zip` | `$.adTargetingParameters.obj_zipCode` | Text. |
| district | `$.geo_ot` | `$.adTargetingParameters.obj_regio4`, `$.normalized.address.region` | Text. |
| city | `$.normalized.address.region` | `$.city` | Keep input city fallback at SQL layer if null. |
| country_code | `$.countryCode` | `$.normalized.countryCode` | Uppercase two-letter code. |
| price_monthly | `$.normalized.price.amount` | `$.adTargetingParameters.obj_rentPerMonth`, `$.obj_totalRent` | Numeric parse if string-formatted. |
| price_per_m2 | `$.adTargetingParameters.obj_rentPerSqM` | `$.obj_baseRent`, `$.pricePerM2` | Numeric parse if string-formatted. |
| surface_m2 | `$.adTargetingParameters.obj_mainFloorSpace` | `$.normalized.area.livingSpace`, `$.obj_netFloorSpace` | Numeric parse. |
| contact_name | `$.normalized.contact.name` | `$.contact.contactData.agent.name`, `$.obj_contactName` | Text. |
| company_name | `$.normalized.contact.company` | `$.contact.contactData.agent.company`, `$.obj_realtorCompanyName` | Text. |
| phone | `$.normalized.contact.phone` | `$.contact.phoneNumbers[0].text`, `$.obj_phoneNumber` | Text, keep raw formatting. |

## Source: idealista

Observed run: `local-barcelona-001` (Barcelona).

| Target field | Primary JSON path | Fallback(s) | Notes |
|---|---|---|---|
| external_id | `$.adid` | `$.basicInfo.propertyCode` | String cast. |
| listing_url | `$.detailWebLink` | `$.basicInfo.url` | Keep full URL. |
| latitude | `$.ubication.latitude` | `$.basicInfo.latitude` | Use `TRY_CAST(float)`. |
| longitude | `$.ubication.longitude` | `$.basicInfo.longitude` | Use `TRY_CAST(float)`. |
| address | `$.ubication.title` | `$.basicInfo.address` | Text. |
| postal_code | `$.contactInfo.address.postalCode` | `$.basicInfo.locationId` (last-resort only) | Prefer explicit postal code. |
| district | `$.basicInfo.district` | `$.ubication.administrativeAreaLevel3`, `$.basicInfo.neighborhood` | Text. |
| city | `$.basicInfo.municipality` | `$.ubication.administrativeAreaLevel2`, input city | Text. |
| country_code | `$.country` | `$.basicInfo.country` | Lowercase in payload; normalize to uppercase. |
| price_monthly | `$.price` | `$.priceInfo.amount`, `$.basicInfo.price` | Numeric. |
| price_per_m2 | `$.priceByArea` | `$.basicInfo.priceByArea` | Numeric. |
| surface_m2 | `$.moreCharacteristics.constructedArea` | `$.basicInfo.size` | Numeric. |
| contact_name | `$.contactInfo.contactName` | `$.contactInfo.agentInfo.name` | Text. |
| company_name | `$.contactInfo.commercialName` | `$.basicInfo.contactInfo.commercialName` | Text. |
| phone | `$.contactInfo.phone1.phoneNumberForMobileDialing` | `$.contactInfo.phone1.formattedPhone`, `$.contactInfo.phone1.phoneNumber` | Prefer E.164-like field. |

## Source: otodom

Observed run: `local-warsaw-001` (Warsaw).

| Target field | Primary JSON path | Fallback(s) | Notes |
|---|---|---|---|
| external_id | `$.id` | — | String cast. |
| listing_url | `$.propertyUrl` | — | Keep full URL. |
| latitude | `$.latitude` | — | Use `TRY_CAST(float)`. |
| longitude | `$.longitude` | — | Use `TRY_CAST(float)`. |
| address | `$.street` | `$.location` | Prefer street if present. |
| postal_code | — | — | Not present in sampled payload. |
| district | `$.district` | `$.subdistrict` | Text. |
| city | `$.city` | input city | In payload this is often `Warszawa` (Polish spelling). |
| country_code | — | input-country mapping (`PL`) | No explicit country code field observed in sample rows. |
| price_monthly | `$.price` | `$.rentPrice` | Numeric. |
| price_per_m2 | `$.pricePerM2` | — | Numeric. |
| surface_m2 | `$.area` | — | Numeric. |
| contact_name | first non-agency key from `$.sellerPhones` | `$.sellerName`, `$.contactName`, `$.advertiserName` | Otodom usually stores individual names as object keys in `sellerPhones`. |
| company_name | `$.agencyName` | — | Text. |
| phone | value from `$.sellerPhones[contact_name]` | `$.sellerPhone`, first value from `$.sellerPhones` object | Prefer individual contact phone when `contact_name` is inferred from `sellerPhones`. |

## Notes For SQL View Implementation

- Always `TRY_CAST` numeric candidates.
- Use `COALESCE` in declared priority order per source.
- Keep `source`, `run_id`, `item_index`, `inserted_at` from bronze row metadata for traceability.
- Keep raw fallback ability: any downstream schema change should update only the view mapping, never raw ingestion.
- Lusha email slots are not available in `payload_json`; materialization joins the existing bronze building/listing/contact tables and reuses already-enriched contacts.
- Idealista country fallback must use configured run city (`milan` -> `IT`, Spanish cities -> `ES`) instead of a source-level `idealista -> ES` default.

## App Read Recommendation

For application reads (globe), prefer the materialized table:

- `silver.location_scraper_globe_v2`

refreshed automatically by Function App activity:

- `ls_materialize_globe` (per run)

DDL script:

- `scripts/sql_scripts/location_scraper_globe_materialized_v2.sql`
