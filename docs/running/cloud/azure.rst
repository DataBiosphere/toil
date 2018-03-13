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
   Az4re Autoscaling is in beta! It is currently only tested with the AWS job store.
   More work is on the way to fix this.

The steps to run a Azure workflow are similar to those of AWS (:ref:`Autoscaling`), except you will
need to explicitly specify the ``--provisioner azure`` option which otherwise defaults to ``aws``.

#. Download :download:`sort.py <../../../src/toil/test/sort/sort.py>`.

#. Launch the leader node in Azure using the :ref:`launchCluster` command. ::

    (venv) $ toil launch-cluster <CLUSTER-NAME> --provisioner azure --leaderNodeType Standard_A2 --keyPairName <SSH-KEYNAME> --zone westus

   Where ``<SSH-KEYNAME>`` is the first part of ``[USERNAME]`` used when setting up your ssh key (see
   :ref:`prepareGoogle`). For example if ``[USERNAME]`` was jane@example.com, ``<SSH-KEYNAME>`` should be ``jane``.


   The ``--keyPairName`` option is for an SSH key that was added to the Google account. If your ssh
   key ``[USERNAME]`` was ``jane@example.com``, then your key pair name will be just ``jane``.

#. Upload the sort example and ssh into the leader. ::

    (venv) $ toil rsync-cluster --provisioner gce <CLUSTER-NAME> sort.py :/root
    (venv) $ toil ssh-cluster --provisioner gce <CLUSTER-NAME>

#. Run the workflow. ::

    $ python /root/sort.py  aws:us-west-2:<JOBSTORE-NAME> --provisioner gce --batchSystem mesos --nodeTypes n1-standard-2 --maxNodes 2

#. Cleanup ::

    $ exit  # this exits the ssh from the leader node
    (venv) $ toil destory-cluster --provisioner gce <CLUSTER-NAME>

.. _Google's Instructions: https://cloud.google.com/docs/authentication/getting-started
