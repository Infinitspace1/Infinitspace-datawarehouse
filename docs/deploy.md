
To deploy to Azure Func:
func azure functionapp publish func-infinitspace-datawarehouse --python

Create raw Nexudus blob container:
az storage container create --name nexudus-raw-snapshots --account-name staccinfinitspaceprod001 --auth-mode login

Set blob settings on the Function App:
az functionapp config appsettings set --resource-group infinitspace-prod-northeurope-data-rg --name func-infinitspace-datawarehouse --settings AZURE_STORAGE_ACCOUNT_NAME=staccinfinitspaceprod001 AZURE_STORAGE_CONTAINER_RAW_NEXUDUS=nexudus-raw-snapshots

Grant Function identity data-plane access (one-time):
az role assignment create --assignee c7182846-ab9c-44bf-9a54-b94515e95f4f --role "Storage Blob Data Contributor" --scope /subscriptions/5aba9bec-653f-4832-a4e8-1de98efc8e8d/resourceGroups/infinitspace-prod-northeurope-data-rg/providers/Microsoft.Storage/storageAccounts/staccinfinitspaceprod001


To remove deny rule (to test connections):
az functionapp config access-restriction remove --resource-group infinitspace-prod-northeurope-data-rg --name func-infinitspace-datawarehouse --rule-name "DenyAllPublic" --action Deny

To put rule deny access:
az functionapp config access-restriction add --resource-group infinitspace-prod-northeurope-data-rg --name func-infinitspace-datawarehouse --rule-name "DenyAllPublic" --action Deny --priority 100 --ip-address 0.0.0.0/0