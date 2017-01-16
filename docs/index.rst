Toil
====

Toil is an open-source pure-Python workflow engine that lets people write better
pipelines. You can:

* Write your workflows in `Common Workflow Language`_ (CWL),
* Run workflows on your laptop or on huge commercial clouds such as
  `Amazon Web Services`_ (including the `spot market`_), `Microsoft Azure`_,
  `OpenStack`_, and `Google Compute Engine`_,
* Take advantage of high-performance computing environments with batch systems
  like `GridEngine`_, `Apache Mesos`_, and `Parasol`_,
* Run workflows concurrently at scale using hundreds of nodes and thousands of cores,
* Execute workflows efficiently with caching and resource requirement specifications, and
* Easily link databases and services

Toil is, admittedly, not quite as good as sliced bread, but it's as close to it
as you're gonna get. Click `here`_ to learn more about Toil and what it can do,
or jump in and skip to :ref:`Installation`. (You can also join us on `GitHub`_
or `Gitter`_.)

.. _GridEngine: http://gridscheduler.sourceforge.net/
.. _Parasol: http://genecats.soe.ucsc.edu/eng/parasol.html
.. _Apache Mesos: http://mesos.apache.org/
.. _spot market: https://aws.amazon.com/ec2/spot/
.. _Microsoft Azure: https://azure.microsoft.com
.. _Amazon Web Services: https://aws.amazon.com/
.. _Google Compute Engine: https://cloud.google.com/compute/
.. _OpenStack: https://www.openstack.org/
.. _GitHub: https://github.com/BD2KGenomics/toil
.. _Gitter: https://gitter.im/bd2k-genomics-toil/Lobby
.. _here: https://bd2kgenomics.github.io/toil/

Getting Started
~~~~~~~~~~~~~~~

.. toctree::
   :maxdepth: 2

   install/basic
   install/cloud
   running/running
   running/cloud

User Guide
~~~~~~~~~~

.. toctree::
   :maxdepth: 2

   cli
   developing
   deploying

API and Architecture
~~~~~~~~~~~~~~~~~~~~

.. toctree::
   :maxdepth: 2

   toilAPI
   architecture
   batchSystem
   jobStore
   contributors/install
   contributors/guidelines

Indices and tables
~~~~~~~~~~~~~~~~~~

* :ref:`genindex`
* :ref:`search`
