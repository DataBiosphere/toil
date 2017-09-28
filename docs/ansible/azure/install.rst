.. highlight:: console

.. _azureInstallation-ref:

Install Azure CLI
=================
In this section, you will install the Azure CLI and configure it so that you can provision and monitor your Azure resources from the command line.

Install Azure
-------------
1. Install the Azure CLI using ``pip``::

   $ pip install azure-cli


2. Verify that the version of the Azure CLI is 2.0 or higher::

   $ az --version

3. Install a Python DNS resolver using ``pip``::

   $ pip install dnspython

4. Install a REST client for Azure using ``pip``::

   $ pip install msrestazure


Create an Azure service principal
---------------------------------
1. Create an Active Directory application::

   $ az ad app create --password <password> --display-name <display-name> --homepage <home-page> --identifier-uris <identifier-uri>

   e.g.::

   $ az ad app create --password abc123 --display-name jcApp --home-page jcApp.jc.com --identifier-uris jcApp.jc.com

   Your expected output will be similar to the following::

     {
     "appId": "11111111-1111-1111-1111-111111111111",
     "appPermissions": null,
     "availableToOtherTenants": false,
     "displayName": "jcApp",
     "homepage": "jcApp.jc.com",
     "identifierUris": [ "jcApp.jc.com" ],
     "objectId": "55555555-5555-5555-5555-555555555555",
     "objectType": "Application",
     "replyUrls": []
     }

2. Create an Azure service principal for the application::

   $ az ad sp create --id <appId>

   e.g.::

   $ az ad sp create --id 11111111-1111-1111-1111-111111111111

   Your expected output will be similar to the following::

     {
     "appId": "11111111-1111-1111-1111-111111111111",
     "displayName": "ansibleApp",
     "objectId": "44444444-4444-4444-4444-444444444444",
     "objectType": "ServicePrincipal",
     "servicePrincipalNames": ["11111111-1111-1111-1111-111111111111", "ansible.mydomain.com"
     ]
     }

   Note the value associated with the ``objectId`` -- you'll need that in a subsequent step.

3. Identify your Azure subscription ID and tenant ID::

   $ ad account show

   Your expected output will be similar to the following::

     {
     "environmentName": "AzureCloud",
     "id": "22222222-2222-2222-2222-222222222222",
     "isDefault": true,
     "name": "JC Subscription Name",
     "state": "Enabled",
     "tenantId": "33333333-3333-3333-3333-333333333333",
     "user": { "name": "j.c@microsoft.com", "type": "user" }
     }

4. Assign the Azure Contributor role to the service principal associated with the default resource group::

   $ az role assignment create --assignee <objectId> --role contributor

   e.g.::

   $ az role assignment create --assignee 44444444-4444-4444-4444-444444444444 --role contributor

   Your expected output will be similar to the following::

      {
      "id": "/subscriptions/22222222-2222-2222-2222-222222222222/resourceGroups/ansiblelab/providers/Microsoft.Authorization/roleAssignments/66666666-6666-6666-6666-666666666666",
      "name": "66666666-6666-6666-6666-666666666666",
      "properties": {
      "principalId": "44444444-4444-4444-4444-444444444444",
      "roleDefinitionId": "/subscriptions/22222222-2222-2222-2222-222222222222/providers/Microsoft.Authorization/roleDefinitions/77777777-7777-7777-7777-777777777777",
      "scope": "/subscriptions/22222222-2222-2222-2222-222222222222/resourceGroups/ansiblelab"
      },
      "resourceGroup": "ansiblelab",
      "type": "Microsoft.Authorization/roleAssignments"
      }

Create Azure network resources
------------------------------
1. Login to Azure::

   $ az login

2. Create a resource group in the location nearest you::

   $ ansible group create --name <resource-group-name> --location <location>

   e.g.::

   $ az group create --name jcResourceGroup --location westus

3. Set the resource group you just created to be your default resource group::

   $ az configure --defaults group=<resource-group-name>

   e.g.::

   $ az configure --defaults group=jcResourceGroup

4. Create a virtual network in which your virtual machines will run::

   $ az network vnet create -n <virtual-network-name> --address-prefixes <cidr-network> --subnet-name <subnet-name> --subnet-prefix <subnet-prefix>

   e.g.::

   $ az network vnet create -n jcVnet --address-prefixes 192.168.0.0/16 --subnet-name jcSubnet --subnet-prefix 192.168.1.0/24



Validate your Azure CLI installation and configuration
------------------------------------------------------

1. Create a public IP address for your VM::

   $ az network public-ip create --name <ip-name>


   e.g.::

   $ az network public-ip create --name jcIP

   Your expected output will be similar to the following::

      {
      "fqdns": "",
      "id": "/subscriptions/3e78e84b-6750-44b9-9d57-d9bba935237a/resourceGroups/jcResourceGroup/providers/Microsoft.Compute/virtualMachines/ansibleMaster",
      "location": "westus",
      "macAddress": "00-0D-3A-24-E2-C0",
      "powerState": "VM running",
      "privateIpAddress": "192.168.1.4",
      "publicIpAddress": "1.2.3.4",
      "resourceGroup": "jcResourceGroup"
      }



2. Create a VM in Azure::

   $ az vm create -n jctestvm --image OpenLogic:CentOS:7.3:latest --vnet-name jcVnet --subnet jcSubnet --public-ip-address jcIP --authentication-type password --admin-username admin --admin-password abc123
