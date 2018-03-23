.. _runningAzure:

Running in Azure
================

This section describes how to deploy a leader node in Azure and launch a Toil cluster from the leader node to run
workflows.  You'll need an account in Azure prior to executing the steps in the document.  To setup Azure, see
:ref:`prepareAzure`.

.. note::
   Azure support in Toil is still experimental and in Beta!

.. _prepareAzure:

Preparing your Azure environment
--------------------------------

Follow the steps below to prepare your Azure environment for running a Toil workflow.

#. Create an `Azure account`_ and an `Azure storage account`_.

#. Locate your Azure storage account key and then store it in one of the following locations:
    - ``AZURE_ACCOUNT_KEY_<account>`` environment variable
    - ``AZURE_ACCOUNT_KEY`` environment variable
    - or finally in ``~/.toilAzureCredentials.`` with the format ::

         [AzureStorageCredentials]
         accountName1=ACCOUNTKEY1
         accountName2=ACCOUNTKEY2

   These locations are searched in the order above, which can be useful if you work with multiple accounts.  Only one
   is needed however.

#. Create an `Azure active directory and service principal`_ to create the following credentials:
    - Client ID (also known as an Application ID)
    - Subscription ID (found via "All Services", then "Subscriptions")
    - secret (also known as the authentication key)
    - Tenant ID (also known as a Directory ID)

   These Azure credentials need to be stored a location where the Ansible scripts can read them.
   There are multiple options for doing so as described here_, one of which is to create a
   file called `~/.azure/credentials` with the format ::

      [default]
      subscription_id=22222222-2222-2222-2222-222222222222
      client_id=1111111-111-1111-111-111111111
      secret=abc123
      tenant=33333333-3333-3333-3333-333333333333

#. Add the "app" you created as a user with appropriate permissions by going to "All Services", then "Permissions",
   then "Access Control (IAM)", then "+ Add", then selecting "Owner" from the "Role" drop-down menu on the right,
   then typing in the name of your app under "Select" and add permissions.

#. Create an SSH keypair if one doesn't exist (see: `add SSH`_).

.. note::
   You do not need to upload your key pair to Azure as with AWS.

.. _add SSH: https://help.github.com/articles/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent/
.. _Azure account: https://azure.microsoft.com/en-us/free/
.. _here: http://docs.ansible.com/ansible/latest/guide_azure.html#providing-credentials-to-azure-modules.o/docs/py2or3.html
.. _Azure storage account: https://docs.microsoft.com/en-us/azure/storage/common/storage-quickstart-create-account?tabs=portal
.. _Azure active directory and service principal: https://docs.microsoft.com/en-us/azure/azure-resource-manager/resource-group-create-service-principal-portal

Running a Workflow with Autoscaling
-----------------------------------

.. warning::
   Azure Autoscaling is in beta!

The steps to run a Azure workflow are similar to those of AWS (:ref:`Autoscaling`), except you will
need to explicitly specify the ``--provisioner azure`` option which otherwise defaults to ``aws``.

#. Download :download:`sort.py <../../../src/toil/test/sort/sort.py>`.

#. Launch the leader node in Azure using the :ref:`launchCluster` command. ::

    (venv) $ toil launch-cluster <CLUSTER-NAME> --provisioner azure --leaderNodeType Standard_A2 --keyPairName <OWNER> --zone westus

   For Azure, the provisioner needs the ssh public key. It will read the file given by ``--publicKeyFile``. The
   default location is ~/.ssh/id_rsa.pub. The --keyPairName option is used to indicate the instances owner.
   See `add SSH`_.

#. Upload the sort example and ssh into the leader. ::

    (venv) $ toil rsync-cluster --provisioner azure <CLUSTER-NAME> sort.py :/root
    (venv) $ toil ssh-cluster --provisioner azure <CLUSTER-NAME>

#. Run the workflow. ::

    $ python /root/sort.py  azure:<AZURE-STORAGE-ACCOUNT>:<JOBSTORE-NAME> --provisioner azure --batchSystem mesos --nodeTypes Standard_A2 --maxNodes 2

#. Cleanup ::

    $ exit  # this exits the ssh from the leader node
    (venv) $ toil destroy-cluster --provisioner azure <CLUSTER-NAME>

.. _azureJobStore:

Azure Job Store
---------------

After :ref:`prepareAzure` all you will need to do is specify the job store name prefix for Azure.

For example to run the sort example with Azure job store you would run ::

    $ python sort.py azure:<my-azure-account-name>:my-azure-jobstore
