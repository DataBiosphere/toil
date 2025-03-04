Toil Documentation
==================

Toil is an open-source pure-Python workflow engine that lets people write better pipelines.

Check out our `website`_ for a comprehensive list of Toil's features and read our `paper`_ to learn what Toil can do
in the real world.  Please subscribe to our low-volume `announce`_ mailing list and feel free to also join us on `GitHub`_ and `Gitter`_.

If using Toil for your research, please cite

     Vivian, J., Rao, A. A., Nothaft, F. A., Ketchum, C., Armstrong, J., Novak, A., … Paten, B. (2017).
     Toil enables reproducible, open source, big biomedical data analyses. Nature Biotechnology, 35(4), 314–316.
     http://doi.org/10.1038/nbt.3772

.. _website: http://toil.ucsc-cgl.org/
.. _announce: https://groups.google.com/forum/#!forum/toil-announce
.. _GridEngine: http://gridscheduler.sourceforge.net/
.. _Apache Mesos: http://mesos.apache.org/
.. _spot market: https://aws.amazon.com/ec2/spot/
.. _Amazon Web Services: https://aws.amazon.com/
.. _Google Compute Engine: https://cloud.google.com/compute/
.. _OpenStack: https://www.openstack.org/
.. _GitHub: https://github.com/BD2KGenomics/toil
.. _Gitter: https://gitter.im/bd2k-genomics-toil/Lobby
.. _paper: http://biorxiv.org/content/early/2016/07/07/062497

.. todolist::

.. toctree::
   :caption: Getting Started

   gettingStarted/install
   gettingStarted/quickStart

.. toctree::
   :caption: Common Workflow Language (CWL)

   cwl/introduction
   cwl/running
   cwl/conformance

.. toctree::
   :caption: Workflow Description Language (WDL)
   
   wdl/introduction
   wdl/running
   wdl/developing
   wdl/conformance

.. toctree::
   :caption: Advanced Toil Usage

   running/introduction
   running/cliOptions
   running/utils
   running/debugging
   running/cloud/cloud
   running/hpcEnvironments
   running/server/wes
   
.. toctree::
   :caption: Toil Python API

   python/developing
   python/toilAPI
   python/toilAPIJobstore
   python/toilAPIJobFunctions
   python/toilAPIMethods
   python/toilAPIRunner
   python/toilAPIFilestore
   python/toilAPIBatchsystem
   python/toilAPIService
   python/toilAPIExceptions

.. toctree::
   :caption: Contributing to Toil

   contributing/contributing
   contributing/checklists

.. toctree::
   :caption: Appendices

   appendices/architecture
   appendices/aws_min_permissions
   appendices/deploy
   appendices/environment_vars


* :ref:`genindex`
* :ref:`search`


