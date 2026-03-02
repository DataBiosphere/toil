.. image:: https://flat.badgen.net/https/ucsc-ci.com/api/v4/projects/3/jobs/artifacts/master/raw/badges1.2/required.json%3Fjob=cwl_badge%26search_recent_successful_pipelines=true?icon=commonwl&label=CWL%201.2%20Conformance
   :alt: Toil CWL 1.2 Conformance Badge
   :target: https://github.com/common-workflow-language/cwl-v1.2/blob/main/CONFORMANCE_TESTS.md

.. image:: https://badges.gitter.im/bd2k-genomics-toil/Lobby.svg
   :alt: Join the chat at https://gitter.im/bd2k-genomics-toil/Lobby
   :target: https://gitter.im/bd2k-genomics-toil/Lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge

Toil is a scalable, efficient, cross-platform (Linux & macOS) pipeline management system,
written entirely in Python, and designed around the principles of functional
programming. It supports running workflows written in either Common Workflow Language (`CWL`_) 1.0-1.2 or 
Workflow Description Language (`WDL`_) 1.0-1.1, as well as having its own rich Python API for writing workflows against. 
It supports running workflows locally on your system (e.g. a laptop), on an HPC cluster, or in the cloud. 

* Check the `website`_ for a description of Toil and its features.
* Full documentation for the latest stable release can be found at
  `Read the Docs`_.
* Please subscribe to low-volume `announce`_ mailing list so we keep you informed
* Google Groups discussion `forum`_
* See our occasional `blog`_ for tutorials. 
* Use `biostars`_ channel for discussion.

.. _website: http://toil.ucsc-cgl.org/
.. _Read the Docs: https://toil.readthedocs.io/en/latest
.. _announce: https://groups.google.com/forum/#!forum/toil-announce
.. _forum: https://groups.google.com/forum/#!forum/toil-community
.. _blog: https://toilpipelines.wordpress.com/
.. _biostars: https://www.biostars.org/t/toil/
.. _CWL: https://www.commonwl.org/
.. _WDL: https://openwdl.org/

Notes:

* Toil moved from https://github.com/BD2KGenomics/toil to https://github.com/DataBiosphere/toil on July 5th, 2018.
* Toil dropped Python 2.7 support on February 13, 2020 (the last working py2.7 version is 3.24.0).
