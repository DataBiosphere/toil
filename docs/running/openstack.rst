.. _runningOpenStack:

Running in Openstack
=========================

Our group is working to expand distributed cluster support to OpenStack by providing convenient Docker containers to launch Mesos from. Currently, OpenStack nodes can be set up to run Toil in single machine mode by following the :ref:`installation-ref`.

Toil scripts can be run by designating a job store location.
Be sure to specify a temporary directory that Toil can use to run jobs in with
the ``--workDir`` argument::

    $ python HelloWorld.py --workDir=/tmp file:jobStore

