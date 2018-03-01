.. _runningGCE:

Running in Google Compute Engine (GCE)
==============
After setting up Toil on :ref:`installation-ref`, Toil scripts
can be run just by designating a job store location as shown in
:ref:`quickstart`.

Toil supports using the `Google Cloud Platform`_.
Setting this up is easy! just

#. make sure that the ``google`` extra (:ref:`extras`) is installed and

#. follow `Google's Instructions`_ to download credentials and set the
   ``GOOGLE_APPLICATION_CREDENTIALS`` environment variable.

#. add your ssh keys to the google account `Adding SSH Keys`_

Running a Workflow with Autoscaling
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The steps to run a GCE workflow are similar to those of AWS (:ref:`Autoscaling`).

#. Download :download:`sort.py <../../src/toil/test/sort/sort.py>`.

#. Launch the leader node in GCE using the :ref:`launchCluster` command. ::

    (venv) $ toil launch-cluster gce-sort --provisioner gce --leaderNodeType n1-standard-1 \
              --keyPairName <ssh-keyName> --boto <botoDirL> --zone us-west1-a

The ``boto`` option is necessary to talk to an AWS jobstore (the Google jobStore will be ready with issue #1948).

The ``keyPairName`` option is for an SSH key that was added to the Google account.

#. Upload the sort example and ssh into the leader.
    (venv) $ toil rsync-cluster --provisioner gce gce-sort sort.py :/root
    (venv) $ toil ssh-cluster --provisioner gce gce-sort

#. Run the workflow.
    $ python /root/sort.py  aws:us-west-2:gce-sort-jobstore --provisioner gce --batchSystem mesos \
       --mesosMaster <leader-private-ip>:5050 --nodeTypes n1-standard-2 --maxNodes 2

#. Cleanup
    $ exit
    (venv) $toil destory-cluster --provisioner gce gce-sort


.. _Google's Instructions: https://developers.google.com/identity/protocols/application-default-credentials#howtheywork

.. _Google Cloud Platform: https://cloud.google.com/storage/

.. _Adding SSH Keys: https://cloud.google.com/compute/docs/instances/adding-removing-ssh-keys
