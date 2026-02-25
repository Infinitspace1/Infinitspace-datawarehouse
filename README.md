# InfinitSpace Data Warehouse

**Official ETL repository for the InfinitSpace data warehouse pipeline**

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![Azure Functions](https://img.shields.io/badge/Azure-Functions-0078D4?logo=microsoft-azure)](https://azure.microsoft.com/en-us/services/functions/)
[![License](https://img.shields.io/badge/license-Proprietary-red.svg)]()

---

## 🎯 Overview

The InfinitSpace Data Warehouse is a production-grade ETL pipeline that:

- **Extracts** data from multiple sources (Nexudus, Hubspot, OneDrive, etc.)
- **Loads** raw data into a **Bronze layer** (append-only, immutable)
- **Transforms** data into a **Silver layer** (cleaned, typed, normalized)
- **Merges** data into a **Core layer** (source-agnostic, canonical entities)

The pipeline runs **daily on Azure Functions** with automatic scheduling, error tracking, and monitoring.

---

## 📊 Current Status

### ✅ Implemented

| Feature | Status | Details |
|---------|--------|---------|
| Nexudus → Bronze | ✅ Complete | All 5 entities (locations, products, contracts, resources, extra_services) |
| Bronze → Silver | ✅ Complete | Transformation logic for all entities |
| SQL Schema | ✅ Complete | Bronze, Silver, Core, Meta schemas |
| Local Testing | ✅ Complete | Test scripts for all layers |
| Azure Function (Bronze) | ✅ Complete | Timer trigger at 02:00 UTC daily |
| Azure Function (Silver) | ✅ Complete | Timer trigger at 02:30 UTC daily |
| Run Tracking | ✅ Complete | `meta.sync_runs` + `meta.sync_errors` |
| Documentation | ✅ Complete | Deployment guide, quickstart, schema docs |

### 🚧 Roadmap

| Feature | Priority | Target |
|---------|----------|--------|
| Silver → Core population | High | Q1 2026 |
| Hubspot integration | High | Q2 2026 |
| Incremental loads | Medium | Q2 2026 |
| dbt transformation layer | Medium | Q3 2026 |
| Power BI dashboards | High | Q1 2026 |
| Data quality checks | Medium | Q2 2026 |

---

## 🚀 Quick Start

### 1. Clone & Setup

```bash
git clone <repository-url>
cd Infinitspace-datawarehouse

# Create virtual environment
python -m venv .venv
.venv\Scripts\activate  # Windows
# source .venv/bin/activate  # Linux/Mac

# Install dependencies
pip install -r requirements.txt
```

### 2. Configure Environment

```bash
# Copy template
cp .env.example .env

# Edit .env with your credentials
NEXUDUS_USERNAME=your_username
NEXUDUS_PASSWORD=your_password
AZURE_SQL_CONNECTION_STRING=Driver={ODBC Driver 18 for SQL Server};Server=...
```

### 3. Test Locally

```bash
# Test authentication
python scripts/python_scripts/test_local.py --step auth

# Test SQL connection
python scripts/python_scripts/test_local.py --step sql

# Test full pipeline (dry run)
python scripts/python_scripts/test_local.py --step all --dry-run
```

### 4. Deploy to Azure

```bash
# Setup Azure resources (one-time)
.\deploy\setup_azure_resources.ps1  # Windows
# bash deploy/setup_azure_resources.sh  # Linux/Mac

# Deploy functions
func azure functionapp publish infinitspace-dw-functions --build remote --python
```

**For detailed instructions, see:**
- 📘 [QUICKSTART.md](QUICKSTART.md) - Get up and running in 15 minutes
- 📖 [DEPLOYMENT_GUIDE.md](DEPLOYMENT_GUIDE.md) - Complete deployment documentation

---

## 📁 Repository Structure

```
Infinitspace-datawarehouse/
│
├── 📂 functions/                        Azure Functions (deployed to cloud)
│   ├── bronze/nexudus_to_bronze/       ← Bronze ingestion (Timer: 02:00 UTC)
│   │   ├── function_app.py
│   │   └── host.json
│   └── silver/bronze_to_silver/        ← Silver transformation (Timer: 02:30 UTC)
│       ├── function_app.py
│       └── host.json
│
├── 📂 shared/                           Shared Python modules
│   ├── azure_clients/
│   │   ├── bronze_writer.py            ← Write raw JSON to bronze
│   │   ├── silver_writer_*.py          ← Transform & upsert to silver
│   │   ├── sql_client.py               ← SQL connection manager
│   │   └── run_tracker.py              ← Log to meta.sync_runs
│   └── nexudus/
│       ├── auth.py                     ← API authentication
│       ├── client.py                   ← API client with rate limiting
│       └── transformers/               ← Bronze → Silver transformations
│           ├── contracts.py
│           ├── products.py
│           ├── locations.py
│           └── extra_services.py
│
├── 📂 scripts/python_scripts/           Local testing & inspection scripts
│   ├── test_local.py                   ← Test pipeline locally
│   ├── test_*_silver.py                ← Test silver transformations
│   └── inspect_*.py                    ← Inspect database content
│
├── 📂 docs/                             Documentation
│   └── silver_table_relationships.md   ← Schema & relationship docs
│
├── 📂 deploy/                           Deployment automation
│   ├── setup_azure_resources.sh        ← Bash deployment script
│   └── setup_azure_resources.ps1       ← PowerShell deployment script
│
├── 📄 requirements.txt                  Python dependencies
├── 📄 .env.example                      Environment variable template
├── 📄 .funcignore                       Files to exclude from deployment
├── 📄 DEPLOYMENT_GUIDE.md               Complete deployment documentation
├── 📄 QUICKSTART.md                     Quick start guide
├── 📄 README.md                         This file
└── 📄 SQL_datawarehouse.md              SQL schema overview
```

---

## 🏗️ Architecture

### Data Flow

```
┌──────────────────────────────────────────────────────────────────┐
│                      DATA SOURCES                                │
├──────────────────────────────────────────────────────────────────┤
│  Nexudus API  │  Hubspot API  │  OneDrive  │  Microsoft 365    │
└────────┬──────┴───────┬───────┴──────┬─────┴─────────┬──────────┘
         │              │              │               │
         │ (Timer:      │  (Future)    │   (Future)   │  (Future)
         │  02:00 UTC)  │              │               │
         ▼              ▼              ▼               ▼
┌──────────────────────────────────────────────────────────────────┐
│                   BRONZE LAYER (Azure SQL)                       │
│  Raw, append-only, immutable storage                             │
├──────────────────────────────────────────────────────────────────┤
│  bronze.nexudus_locations      bronze.hubspot_contacts           │
│  bronze.nexudus_products       bronze.onedrive_files             │
│  bronze.nexudus_contracts      ...                               │
│  bronze.nexudus_resources                                        │
│  bronze.nexudus_extra_services                                   │
└────────┬─────────────────────────────────────────────────────────┘
         │
         │ (Timer: 02:30 UTC)
         │ Transform, clean, type
         ▼
┌──────────────────────────────────────────────────────────────────┐
│                   SILVER LAYER (Azure SQL)                       │
│  Cleaned, typed, normalized data (upserted)                      │
├──────────────────────────────────────────────────────────────────┤
│  silver.nexudus_locations + location_hours                       │
│  silver.nexudus_products                                         │
│  silver.nexudus_contracts                                        │
│  silver.nexudus_resources                                        │
│  silver.nexudus_extra_services                                   │
└────────┬─────────────────────────────────────────────────────────┘
         │
         │ (Future: 03:00 UTC)
         │ Merge, deduplicate
         ▼
┌──────────────────────────────────────────────────────────────────┐
│                    CORE LAYER (Azure SQL)                        │
│  Source-agnostic, canonical business entities                    │
├──────────────────────────────────────────────────────────────────┤
│  core.locations (merged from all sources)                        │
│  core.contracts                                                  │
│  core.products                                                   │
│  core.contacts (future: Nexudus coworkers + Hubspot contacts)    │
└────────┬─────────────────────────────────────────────────────────┘
         │
         │ Consumed by:
         ▼
┌──────────────────────────────────────────────────────────────────┐
│  Power BI Dashboards  │  Ava Bot  │  Internal Tools  │  APIs     │
└──────────────────────────────────────────────────────────────────┘
```

### Azure Resources

```
Resource Group: infinitspace-datawarehouse-prod
│
├── 🗄️ Azure SQL Database
│   └── infinitspace-prod-main-db
│       ├── Schema: bronze (raw data)
│       ├── Schema: silver (cleaned data)
│       ├── Schema: core (canonical data)
│       └── Schema: meta (tracking & logs)
│
├── ⚡ Azure Function App (Consumption Plan)
│   └── infinitspace-dw-functions
│       ├── nexudus-to-bronze (Timer: 02:00 UTC)
│       └── bronze-to-silver (Timer: 02:30 UTC)
│
├── 📊 Application Insights
│   └── infinitspace-dw-insights (monitoring & logs)
│
├── 🔐 Key Vault
│   └── infinitspace-dw-kv
│       ├── Secret: nexudus-username
│       ├── Secret: nexudus-password
│       └── Secret: sql-connection-string
│
└── 💾 Storage Account
    └── infinitspacedwstorage (function app storage)
```

---

## 🧪 Testing

### Local Testing

```bash
# Test individual steps
python scripts/python_scripts/test_local.py --step auth
python scripts/python_scripts/test_local.py --step locations --limit 10

# Test silver transformations
python scripts/python_scripts/test_locations_silver.py
python scripts/python_scripts/test_products_silver.py
python scripts/python_scripts/test_contracts_silver.py
python scripts/python_scripts/test_extra_services_silver.py

# Inspect database
python scripts/python_scripts/inspect_bronze.py
```

### Azure Testing

```bash
# Manual function trigger
az functionapp function invoke \
  --name infinitspace-dw-functions \
  --resource-group infinitspace-datawarehouse-prod \
  --function-name nexudus-to-bronze

# Monitor logs
az functionapp log tail \
  --name infinitspace-dw-functions \
  --resource-group infinitspace-datawarehouse-prod
```

### SQL Validation

```sql
-- Check latest sync runs
SELECT TOP 10 * FROM meta.sync_runs ORDER BY started_at DESC;

-- Verify data counts
SELECT 'bronze.locations' AS table_name, COUNT(*) AS row_count FROM bronze.nexudus_locations
UNION ALL
SELECT 'silver.locations', COUNT(*) FROM silver.nexudus_locations
UNION ALL
SELECT 'silver.products', COUNT(*) FROM silver.nexudus_products
UNION ALL
SELECT 'silver.contracts', COUNT(*) FROM silver.nexudus_contracts;

-- Check for errors
SELECT * FROM meta.sync_runs WHERE status = 'failed' ORDER BY started_at DESC;
```

---

## 📊 Monitoring

### Key Metrics

- **Function Execution:** Track runs, duration, success rate in Application Insights
- **Data Freshness:** Monitor `meta.sync_runs.finished_at` for each entity
- **Error Rate:** Alert on `status='failed'` in `meta.sync_runs`
- **Data Volume:** Track row counts in bronze/silver tables

### Application Insights Queries

```kusto
// Function success rate (last 7 days)
requests
| where cloud_RoleName == "infinitspace-dw-functions"
| where timestamp > ago(7d)
| summarize runs=count(), success_rate=countif(success==true)*100.0/count() by name

// Recent errors
exceptions
| where cloud_RoleName == "infinitspace-dw-functions"
| where timestamp > ago(24h)
| project timestamp, operation_Name, outerMessage
```

### Alerts (Recommended)

1. **Function Failure:** Any failed execution → Email to data team
2. **No Data:** No successful run in 25 hours → Email + SMS
3. **Long Duration:** Function runs > 10 minutes → Email notification

---

## 🔒 Security & Best Practices

### ✅ Implemented

- Secrets stored in Azure Key Vault (not in code or environment)
- Managed Identity for Key Vault access
- SQL connection uses encrypted connections
- SQL firewall allows only Azure services
- `.env` file excluded from git (`.gitignore`)

### 🔐 Best Practices

- **Never commit secrets:** Always use `.env` locally and Key Vault in Azure
- **Use Managed Identity:** Avoid storing credentials when possible
- **Rotate credentials:** Update Nexudus password quarterly
- **Monitor access:** Review Key Vault access logs monthly
- **Least privilege:** Grant minimum required SQL permissions

---

## 🛠️ Maintenance

### Daily Tasks

- [ ] Check `meta.sync_runs` for failed executions
- [ ] Verify data freshness (last successful run < 25 hours ago)

### Weekly Tasks

- [ ] Review Application Insights for performance trends
- [ ] Check error logs for recurring issues
- [ ] Validate data quality (spot checks on key tables)

### Monthly Tasks

- [ ] Analyze bronze table growth (consider archiving old data)
- [ ] Review Azure costs (function executions, storage)
- [ ] Update dependencies (`pip list --outdated`)

### Quarterly Tasks

- [ ] Rotate Nexudus API credentials
- [ ] Review and optimize SQL indexes
- [ ] Update documentation
- [ ] Disaster recovery drill (restore from backup)

---

## 📚 Documentation

| Document | Purpose |
|----------|---------|
| [DEPLOYMENT_GUIDE.md](DEPLOYMENT_GUIDE.md) | Complete deployment instructions (50+ pages) |
| [QUICKSTART.md](QUICKSTART.md) | Get started in 15 minutes |
| [SQL_datawarehouse.md](SQL_datawarehouse.md) | SQL schema overview |
| [docs/silver_table_relationships.md](docs/silver_table_relationships.md) | Detailed schema documentation |

---

## 🤝 Contributing

This is an internal repository. For questions or contributions:

1. Create a feature branch: `git checkout -b feature/your-feature-name`
2. Make changes and test locally
3. Update documentation if needed
4. Submit for review

---

## 📞 Support

**Questions?** Contact the InfinitSpace Data Engineering Team

**Issues?** Check:
1. `meta.sync_runs` for execution logs
2. `meta.sync_errors` for record-level errors
3. Application Insights for function logs
4. [DEPLOYMENT_GUIDE.md](DEPLOYMENT_GUIDE.md) troubleshooting section

---

## 📜 License

Proprietary - InfinitSpace  
All rights reserved.

---

**Last Updated:** February 25, 2026  
**Maintainer:** InfinitSpace Data Engineering Team  
**Version:** 1.0.0
