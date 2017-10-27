.. highlight:: console

.. _azureInstallation-ref:

Install Azure CLI
=================
In this section, you will install the Azure CLI and configure it so that you can provision and monitor your Azure resources from the command line.

Install Azure
-------------
Make sure the Toil ``ansible`` extra is installed:::

   (venv) $ pip install toil[azure]

Create an Azure service principal
---------------------------------
#. Login to Azure::

   (venv) $ az login

   You will be prompted to open a Web browser, go to a Web site, and enter a code in the text box presented at that site.  Following that, your browser will be redirected to login the http://login.microsoftonline.com.  Once you login there, your ``az login`` command is complete and your shell session is authenticated with Azure.

#. Create an Active Directory application::

   (venv) $ az ad app create --password abc123 --display-name myApp --homepage myapp.acme.com --identifier-uris myapp.acme.com

   Your expected output will be similar to the following::

      {
      "appId": "1111111-111-1111-111-111111111",
      "appPermissions": null,
      "availableToOtherTenants": false,
      "displayName": "myApp",
      "homepage": "myapp.acme.com",
      "identifierUris": [
        "myapp.acme.com"
      ],
      "objectId": "55555555-5555-5555-5555-555555555555",
      "objectType": "Application",
      "replyUrls": []
      }

#. Create a resource group in a location near you::

   (venv) $ az group create --name <resource-group-name> --location <location>

   e.g.::

   (venv) $ az group create --name myresourcegroup --location westus

#. Set the resource group you just created to be your default resource group::

   (venv) $ az configure --defaults group=<resource-group-name>

   e.g.::

   (venv) $ az configure --defaults group=myresourcegroup

#. Create an Azure service principal for the application::

   (venv) $ az ad sp create --id <appId>

   e.g.::

   (venv) $ az ad sp create --id 11111111-1111-1111-1111-111111111111

   Your expected output will be similar to the following::

     {
     "appId": "11111111-1111-1111-1111-111111111111",
     "displayName": "myApp",
     "objectId": "44444444-4444-4444-4444-444444444444",
     "objectType": "ServicePrincipal",
     "servicePrincipalNames": ["11111111-1111-1111-1111-111111111111", "myApp.myDomain.com"
     ]
     }

   Note the value associated with the ``objectId`` -- you'll need that in a subsequent step.

#. Identify your Azure subscription ID and tenant ID::

   (venv) $ az account show

   Your expected output will be similar to the following::

     {
     "environmentName": "AzureCloud",
     "id": "22222222-2222-2222-2222-222222222222",
     "isDefault": true,
     "name": "My Subscription Name",
     "state": "Enabled",
     "tenantId": "33333333-3333-3333-3333-333333333333",
     "user": { "name": "first.last@microsoft.com", "type": "user" }
     }


#. Assign the Azure Contributor role to the service principal associated with the default resource group::

   (venv) $ az role assignment create --assignee <objectId> --role contributor

   e.g.::

   (venv) $ az role assignment create --assignee 44444444-4444-4444-4444-444444444444 --role contributor

   Your expected output will be similar to the following::

      {
      "id": "/subscriptions/22222222-2222-2222-2222-222222222222/resourceGroups//providers/Microsoft.Authorization/roleAssignments/66666666-6666-6666-6666-666666666666",
      "name": "66666666-6666-6666-6666-666666666666",
      "properties": {
      "principalId": "44444444-4444-4444-4444-444444444444",
      "roleDefinitionId": "/subscriptions/22222222-2222-2222-2222-222222222222/providers/Microsoft.Authorization/roleDefinitions/77777777-7777-7777-7777-777777777777",
      "scope": "/subscriptions/22222222-2222-2222-2222-222222222222/resourceGroups/myresourcegroup"
      },
      "resourceGroup": "myresourcegroup",
      "type": "Microsoft.Authorization/roleAssignments"
      }

Create Azure network resources
------------------------------

#. Create a virtual network in which your virtual machines will run::

   (venv) $ az network vnet create -n <virtual-network-name> --address-prefixes <cidr-network> --subnet-name <subnet-name> --subnet-prefix <subnet-prefix>

   e.g.::

   (venv) $ az network vnet create -n myVnet --address-prefixes 192.168.0.0/16 --subnet-name mySubnet --subnet-prefix 192.168.1.0/24

#. Create a public IP address for your VM::


   (venv) $ az network public-ip create --name <ip-name>


   e.g.::

   (venv) $ az network public-ip create --name myIP

   Your expected output will be similar to the following::

      {
      "fqdns": "",
      "id": "/subscriptions/3e78e84b-6750-44b9-9d57-d9bba935237a/resourceGroups/myresourcegroup/providers/Microsoft.Compute/virtualMachines/ansibleMaster",
      "location": "westus",
      "macAddress": "00-0D-3A-24-E2-C0",
      "powerState": "VM running",
      "privateIpAddress": "192.168.1.4",
      "publicIpAddress": "1.2.3.4",
      "resourceGroup": "myresourcegroup"
      }


Create a virtual machine in Azure
---------------------------------

#. Create a VM in Azure::

   (venv) $ az vm create -n mytestvm --image OpenLogic:CentOS:7.3:latest --vnet-name myVnet --subnet mySubnet --public-ip-address myIP --authentication-type password --admin-username test-user --admin-password Microsoft123!

   Your expected output will be similar to the following::

      {
      "fqdns": "",
      "id": "/subscriptions/4f5c03b8-2875-471b-a13d-ff76381d44a1/resourceGroups/myresourcegroup/providers/Microsoft.Compute/virtualMachines/mytestvm",
      "location": "westus",
      "macAddress": "00-0D-3A-30-AE-79",
      "powerState": "VM running",
      "privateIpAddress": "192.168.1.4",
      "publicIpAddress": "40.118.134.86",
      "resourceGroup": "myresourcegroup",
      "zones": ""
      }


#. Use SSH to login to your new VM::

   (venv) $ ssh <user>@<IP-address>

   e.g.::

   (venv) $ ssh test-user@40.118.134.86


#. Logout of your VM::

   $ exit

#. Delete your VM::

   (venv) $ az vm delete --name mytestvm

Create a credentials file for Azure
-----------------------------------
We will need to store our Azure credentials in some location where the Ansible scripts can read them.  There are multiple options for doing so as described here_.

.. _here: http://docs.ansible.com/ansible/latest/guide_azure.html#providing-credentials-to-azure-modules.o/docs/py2or3.html

#. Create a directory in your home directory called `.azure`::

   (venv) $ mkdir ~/.azure

#. Create a file called `~/.azure/credentials`::

      (venv) $ vi ~/.azure/credentials
      [default]
      subscription_id=22222222-2222-2222-2222-222222222222
      client_id=1111111-111-1111-111-111111111
      secret=abc123
      tenant=33333333-3333-3333-3333-333333333333

