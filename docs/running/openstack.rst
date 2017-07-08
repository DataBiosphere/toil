.. _runningOpenStack:

Running in Openstack
=========================

After setting up Toil on :ref:`installationOpenStack`, Toil scripts can be run
by designating a job store location.
Be sure to specify a temporary directory that Toil can use to run jobs in with
the ``--workDir`` argument::

    $ python HelloWorld.py --workDir=/tmp file:jobStore

