
To deploy to Azure Func:
func azure functionapp publish func-infinitspace-datawarehouse --python


To remove deny rule (to test connections):
az functionapp config access-restriction remove --resource-group infinitspace-prod-northeurope-data-rg --name func-infinitspace-datawarehouse --rule-name "DenyAllPublic" --action Deny

To put rule deny access:
az functionapp config access-restriction add --resource-group infinitspace-prod-northeurope-data-rg --name func-infinitspace-datawarehouse --rule-name "DenyAllPublic" --action Deny --priority 100 --ip-address 0.0.0.0/0