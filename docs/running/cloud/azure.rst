.. _runningAzure:

Running in Azure
================

This section describes how to deploy a leader node in Azure and launch a Toil cluster from the leader node to run
workflows.  You'll need an account in Azure prior to executing the steps in the document.  To setup Azure, see
:ref:`prepareAzure`.

.. _azureJobStore:

Azure Job Store
---------------

After :ref:`prepareAzure` all you will need to do is specify the job store name prefix for Azure.

For example to run the sort example with Azure job store you would run ::

    $ python sort.py azure:<my-azure-account-name>:my-azure-jobstore


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
   See :ref:`sshKey`.

#. Upload the sort example and ssh into the leader. ::

    (venv) $ toil rsync-cluster --provisioner azure <CLUSTER-NAME> sort.py :/root
    (venv) $ toil ssh-cluster --provisioner azure <CLUSTER-NAME>

#. Run the workflow. ::

    $ python /root/sort.py  azure:<AZURE-STORAGE-ACCOUNT>:<JOBSTORE-NAME> --provisioner azure --batchSystem mesos --nodeTypes Standard_A3 --maxNodes 2

#. Cleanup ::

    $ exit  # this exits the ssh from the leader node
    (venv) $ toil destroy-cluster --provisioner azure <CLUSTER-NAME>

