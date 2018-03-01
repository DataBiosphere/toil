.. _runningGCE:

Running in Google Compute Engine (GCE)
======================================

Toil supports a provisioner with Google, and a :ref:`googleJobStore`. To get started, follow instructions
for :ref:`prepareGoogle`.

.. _googleJobStore:

Google Job Store
----------------

To use the Google Job Store you will need to set the
``GOOGLE_APPLICATION_CREDENTIALS`` environment variable by following `Google's instructions`_.

Then to run the sort example with the Google job store you would type ::

    $ python sort.py google:my-project-id:my-google-sort-jobstore

Running a Workflow with Autoscaling
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. warning::
   Google Autoscaling is in beta! It is currently only tested with the AWS job store.
   More work is on the way to fix this.

The steps to run a GCE workflow are similar to those of AWS (:ref:`Autoscaling`), except you will
need to explicitly specify the ``--provisioner gce`` option which otherwise defaults to ``aws``.

#. Download :download:`sort.py <../../../src/toil/test/sort/sort.py>`.

#. Launch the leader node in GCE using the :ref:`launchCluster` command. ::

    (venv) $ toil launch-cluster gce-sort --provisioner gce --leaderNodeType n1-standard-1 \
              --keyPairName <ssh-keyName> --boto <botoDir> --zone us-west1-a

   The ``--boto`` option is necessary only if using an AWS jobstore. It also requires that your aws credentials
   are actually saved in your ``.boto`` file.
   (the Google jobStore will be ready with issue #1948).

   The ``--keyPairName`` option is for an SSH key that was added to the Google account. If your ssh
   key ``[USERNAME]`` was ``jane@example.com``, then your key pair name will be just ``jane``.

#. Upload the sort example and ssh into the leader. ::

    (venv) $ toil rsync-cluster --provisioner gce gce-sort sort.py :/root
    (venv) $ toil ssh-cluster --provisioner gce gce-sort

#. Run the workflow. ::

    $ python /root/sort.py  aws:us-west-2:gce-sort-jobstore --provisioner gce --batchSystem mesos --nodeTypes n1-standard-2 --maxNodes 2

#. Cleanup ::

    $ exit
    (venv) $ toil destory-cluster --provisioner gce gce-sort

.. _Google's Instructions: https://cloud.google.com/docs/authentication/getting-started


