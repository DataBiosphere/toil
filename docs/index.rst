Toil Documentation
==================

Toil is an open-source pure-Python workflow engine that lets people write better pipelines.

Check out our `website`_ for a comprehensive list of Toil's features and read our `paper`_ to learn what Toil can do in the real world.  Feel free to also join us on `GitHub`_ and `Gitter`_.

.. _website: http://toil.ucsc-cgl.org/
.. _GridEngine: http://gridscheduler.sourceforge.net/
.. _Parasol: http://genecats.soe.ucsc.edu/eng/parasol.html
.. _Apache Mesos: http://mesos.apache.org/
.. _spot market: https://aws.amazon.com/ec2/spot/
.. _Amazon Web Services: https://aws.amazon.com/
.. _Google Compute Engine: https://cloud.google.com/compute/
.. _OpenStack: https://www.openstack.org/
.. _GitHub: https://github.com/BD2KGenomics/toil
.. _Gitter: https://gitter.im/bd2k-genomics-toil/Lobby
.. _paper: http://biorxiv.org/content/early/2016/07/07/062497
.. _Microsoft Azure: https://azure.microsoft.com/

.. todolist::

.. toctree::
   :caption: Getting Started
   :maxdepth: 2

   install/basic
   running/running

.. toctree::
   :caption: Running Toil
   :maxdepth: 2

   cli
   cwl
   running/cloud
   deploying
   running/amazon
   running/azure
   running/openstack
   running/gce


.. toctree::
   :caption: Developing Toil Workflows
   :maxdepth: 2

   developing
   toilAPI
   batchSystem
   jobStore

.. toctree::
   :caption: Developer's Guide
   :maxdepth: 2

   contributing/contributing
   architecture


.. toctree::
   :caption: Appendices
   :maxdepth: 2

   appendices/environment_vars

* :ref:`genindex`
* :ref:`search`


