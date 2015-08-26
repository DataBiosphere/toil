Toil is a massively scalable pipeline management system, written entirely in Python.
Toil runs as easily on a laptop as it does on a bare-metal cluster or in the cloud, thanks
to support for many batch systems, including `Grid Engine`_, Parasol_, and a
custom Mesos_ framework.

Toil is robust, and designed to run in highly unreliable computing environments like
Amazon's `Spot Market`_. Towards this goal, Toil does not rely on a distributed file system.
Instead, Toil abstracts a pipeline's global storage as a JobStore that can be stored
either locally or on AWS. The result of this abstraction is a robust system that can be
resumed even after an unexpected shutdown of every node in the cluster that resulted in the
loss of all local data.

Writing a Toil script requires only a knowledge of basic Python, with Toil "Jobs" as the
elemental unit of work in a Toil workflow. A Job can dynamically spawn other Jobs as needed,
leading to an intuitive and powerful control over the pipeline.

.. _Grid Engine: http://gridscheduler.sourceforge.net/
.. _Parasol: https://users.soe.ucsc.edu/~donnak/eng/parasol.htm
.. _Mesos: http://mesos.apache.org/
.. _Spot Market: https://aws.amazon.com/ec2/spot/

Prerequisites
=============

Python 2.5 < 3.0

pip_ 7.x

`Apache Mesos`_ 0.22.1, if using the Mesos batch system. This is Brew_ installable on OSX via::

    brew install mesos

Git_, if cloning from the `Toil Github Repository`_

.. _pip: https://pip.readthedocs.org/en/latest/installing.html
.. _Apache Mesos: http://mesos.apache.org/gettingstarted/
.. _Brew: http://brew.sh/
.. _Git: https://git-scm.com/
.. _Toil Github Repository: https://github.com/BD2KGenomics/toil

Installation
============

Toil uses setuptool's extras_ syntax for dependencies of optional features, like the Mesos
batch system and the AWS JobStore. To install Toil with these extras, specify the features
you would like to include when pip installing::

    pip install toil[aws,mesos]

.. _extras: https://pythonhosted.org/setuptools/setuptools.html#declaring-extras-optional-features-with-their-own-dependencies

Building & Testing
==================

This is only required if cloning from Git. Running::

    make develop

will install Toil in editable mode. You can also specify extras to use in develop mode as follows::

    make develop extras=[mesos,aws]

To run the tests, ``cd`` into the toil root directory
and run::

    make test

Finally, running::

    make

by itself will print help for testing and building.