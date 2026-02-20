# DATAWAREHOUSE

bronze is append-only raw storage. You never lose data, and if your transform logic is ever wrong you replay from here. Tables are named bronze.nexudus_locations, bronze.hubspot_contacts, etc. — the source prefix is always part of the name.


silver mirrors bronze in naming but contains typed columns, no JSON. It's always upserted (never rebuilt) using source_id as the unique key. The source_record_id column links every row back to its bronze row for full traceability.

core is source-agnostic — tables are named after business concepts (core.locations, core.contracts, core.products, core.extra_services, core.contacts). Each row has a primary_source + primary_source_id column so you always know where it came from, but the schema doesn't care. 

## DATALAKE 
SQL Server
├── bronze          ← Raw ingestion, one table per entity per source
├── silver          ← Cleaned, typed, normalized (source-aware)  
└── core            ← Production-ready, source-agnostic
    (this is your "lakehouse output" — Ava and others consume from here)

### Example:
{schema}.{source}_{entity}

Examples:
  bronze.nexudus_locations
  bronze.nexudus_contracts
  bronze.hubspot_contacts
  bronze.onedrive_files

  silver.nexudus_locations
  silver.nexudus_contracts
  silver.hubspot_contacts

  core.locations           ← merged/canonical, source-agnostic
  core.contracts
  core.contacts


