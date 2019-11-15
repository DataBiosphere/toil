Toil Documentation
==================

Toil is an open-source pure-Python workflow engine that lets people write better pipelines.

Check out our `website`_ for a comprehensive list of Toil's features and read our `paper`_ to learn what Toil can do
in the real world.  Feel free to also join us on `GitHub`_ and `Gitter`_.

If using Toil for your research, please cite

     Vivian, J., Rao, A. A., Nothaft, F. A., Ketchum, C., Armstrong, J., Novak, A., … Paten, B. (2017).
     Toil enables reproducible, open source, big biomedical data analyses. Nature Biotechnology, 35(4), 314–316.
     http://doi.org/10.1038/nbt.3772

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

.. todolist::

.. toctree::
   :caption: Getting Started
   :maxdepth: 2

   gettingStarted/install
   gettingStarted/quickStart

.. toctree::
   :caption: Running Toil
   :maxdepth: 2

   running/introduction
   running/cliOptions
   running/debugging
   running/cloud/cloud
   running/hpcEnvironments
   running/cwl
   running/wdl

.. toctree::
   :caption: Developing Toil Workflows
   :maxdepth: 2

   developingWorkflows/developing
   developingWorkflows/toilAPI
   developingWorkflows/toilAPIJobstore
   developingWorkflows/toilAPIJobFunctions
   developingWorkflows/toilAPIMethods
   developingWorkflows/toilAPIRunner
   developingWorkflows/toilAPIFilestore
   developingWorkflows/toilAPIBatchsystem
   developingWorkflows/toilAPIService
   developingWorkflows/toilAPIExceptions

.. toctree::
   :caption: Contributing to Toil
   :maxdepth: 2

   contributing/contributing

.. toctree::
   :caption: Appendices
   :maxdepth: 2

   appendices/architecture
   appendices/aws_min_permissions
   appendices/deploy
   appendices/environment_vars

* :ref:`genindex`
* :ref:`search`


