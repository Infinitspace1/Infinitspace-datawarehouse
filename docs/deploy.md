Deploy the default ETL Function App:

```powershell
func azure functionapp publish func-infinitspace-etl --python
```

Set ETL app settings so only the pipeline triggers are registered:

```powershell
az functionapp config appsettings set `
  --resource-group infinitspace-prod-northeurope-data-rg `
  --name func-infinitspace-etl `
  --settings `
    ENABLE_ETL_FUNCTIONS=1 `
    ENABLE_ADMIN_FUNCTIONS=0 `
    AZURE_STORAGE_ACCOUNT_NAME=staccinfinitspaceprod001 `
    AZURE_STORAGE_CONTAINER_RAW_NEXUDUS=nexudus-raw-snapshots
```

Create the raw Nexudus blob container:

```powershell
az storage container create --name nexudus-raw-snapshots --account-name staccinfinitspaceprod001 --auth-mode login
```

Grant the Function identity blob data-plane access:

```powershell
az role assignment create --assignee c7182846-ab9c-44bf-9a54-b94515e95f4f --role "Storage Blob Data Contributor" --scope /subscriptions/5aba9bec-653f-4832-a4e8-1de98efc8e8d/resourceGroups/infinitspace-prod-northeurope-data-rg/providers/Microsoft.Storage/storageAccounts/staccinfinitspaceprod001
```

Optional: deploy a separate admin Function App from the same repo:

```powershell
func azure functionapp publish func-infinitspace-etl --python

az functionapp config appsettings set `
  --resource-group infinitspace-prod-northeurope-data-rg `
  --name func-infinitspace-etl `
  --settings `
    ENABLE_ETL_FUNCTIONS=0 `
    ENABLE_ADMIN_FUNCTIONS=1
```

For Xero OAuth without an HTTP callback, use the CLI flow:

```powershell
python scripts/python_scripts/xero_start_oauth.py --owner-type workspace --owner-id default
python scripts/python_scripts/xero_complete_oauth.py --redirect-url "<full redirect url>"
python scripts/python_scripts/xero_sync_invoices.py --owner-type workspace --owner-id default
```

The production refresh path is DB-backed:
- Tokens are stored encrypted in `meta.xero_connections`.
- Automatic refresh happens inside `shared/xero/client.py`.
- On `invalid_grant`, the connection is marked disconnected instead of retrying forever.

Optional access restriction toggle for an admin app:

```powershell
az functionapp config access-restriction remove --resource-group infinitspace-prod-northeurope-data-rg --name func-infinitspace-etl --rule-name "DenyAllPublic" --action Deny

az functionapp config access-restriction add --resource-group infinitspace-prod-northeurope-data-rg --name func-infinitspace-etl --rule-name "DenyAllPublic" --action Deny --priority 100 --ip-address 0.0.0.0/0
```
