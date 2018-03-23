.. _runningGCE:

Running in Google Compute Engine (GCE)
======================================

Toil supports a provisioner with Google, and a :ref:`googleJobStore`. To get started, follow instructions
for :ref:`prepareGoogle`.

.. _prepareGoogle:

Preparing your Google environment
---------------------------------

Toil supports using the `Google Cloud Platform`_. Setting this up is easy!

#. Make sure that the ``google`` extra (:ref:`extras`) is installed.

#. Follow `Google's Instructions`_ to download credentials and set the
   ``GOOGLE_APPLICATION_CREDENTIALS`` environment variable.

#. Create a new ssh key with the proper format.  To create a new ssh key run the command: ::

       $ ssh-keygen -t rsa -f ~/.ssh/id_rsa -C [USERNAME]

   Where ``[USERNAME]`` is something like ``jane@example.com``. Make sure to leave your password blank.

   .. warning::
       This command could overwrite an old ssh key you may be using.  If you have an existing ssh key
       you would like to use, it will need to be called id_rsa and it needs to have no password set.

   Make sure only you can read the SSH keys: ::

       $ chmod 400 ~/.ssh/id_rsa ~/.ssh/id_rsa.pub

#. Add your newly formated public key to google. To do this, log into your Google Cloud account
   and go to `metadata`_ section under the Compute tab.

   .. image:: googleScreenShot.png

   Near the top of the screen click on 'SSH Keys', then edit, add item, and paste the key. Then save.

   .. image:: googleScreenShot2.png

For more details look at Google's instructions for `adding SSH keys`_

.. _Google Cloud Platform: https://cloud.google.com/storage/
.. _adding SSH keys: https://cloud.google.com/compute/docs/instances/adding-removing-ssh-keys
.. _metadata: https://console.cloud.google.com/compute/metadata
.. _Google's Instructions: https://cloud.google.com/docs/authentication/getting-started

.. _googleJobStore:

Google Job Store
----------------

To use the Google Job Store you will need to set the
``GOOGLE_APPLICATION_CREDENTIALS`` environment variable by following `Google's instructions`_.

Then to run the sort example with the Google job store you would type ::

    $ python sort.py google:my-project-id:my-google-sort-jobstore

Running a Workflow with Autoscaling
-----------------------------------

.. warning::
   Google Autoscaling is in beta!

The steps to run a GCE workflow are similar to those of AWS (:ref:`Autoscaling`), except you will
need to explicitly specify the ``--provisioner gce`` option which otherwise defaults to ``aws``.

#. Download :download:`sort.py <../../../src/toil/test/sort/sort.py>`.

#. Launch the leader node in GCE using the :ref:`launchCluster` command. ::

    (venv) $ toil launch-cluster <CLUSTER-NAME> --provisioner gce --leaderNodeType n1-standard-1 --keyPairName <SSH-KEYNAME> --zone us-west1-a

   Where ``<SSH-KEYNAME>`` is the first part of ``[USERNAME]`` used when setting up your ssh key.  
   For example if ``[USERNAME]`` was jane@example.com, ``<SSH-KEYNAME>`` should be ``jane``.


   The ``--keyPairName`` option is for an SSH key that was added to the Google account. If your ssh
   key ``[USERNAME]`` was ``jane@example.com``, then your key pair name will be just ``jane``.

#. Upload the sort example and ssh into the leader. ::

    (venv) $ toil rsync-cluster --provisioner gce <CLUSTER-NAME> sort.py :/root
    (venv) $ toil ssh-cluster --provisioner gce <CLUSTER-NAME>

#. Run the workflow. ::

    $ python /root/sort.py  google:<PROJECT-ID>:<JOBSTORE-NAME> --provisioner gce --batchSystem mesos --nodeTypes n1-standard-2 --maxNodes 2

#. Cleanup. ::

    $ exit  # this exits the ssh from the leader node
    (venv) $ toil destroy-cluster --provisioner gce <CLUSTER-NAME>

.. _Google's Instructions: https://cloud.google.com/docs/authentication/getting-started


