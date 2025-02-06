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


#### CWL Conformance

![command_line_tool](https://flat.badgen.net/https/ucsc-ci.com/api/v4/projects/3/jobs/artifacts/issues/5183-cwl-badge/raw/badges/command_line_tool.json%3Fjob=cwl_badge)
![conditional](https://flat.badgen.net/https/ucsc-ci.com/api/v4/projects/3/jobs/artifacts/issues/5183-cwl-badge/raw/badges/conditional.json%3Fjob=cwl_badge)
![docker](https://flat.badgen.net/https/ucsc-ci.com/api/v4/projects/3/jobs/artifacts/issues/5183-cwl-badge/raw/badges/docker.json%3Fjob=cwl_badge)
![expression_tool](https://flat.badgen.net/https/ucsc-ci.com/api/v4/projects/3/jobs/artifacts/issues/5183-cwl-badge/raw/badges/expression_tool.json%3Fjob=cwl_badge)
![format_checking](https://flat.badgen.net/https/ucsc-ci.com/api/v4/projects/3/jobs/artifacts/issues/5183-cwl-badge/raw/badges/format_checking.json%3Fjob=cwl_badge)
![initial_work_dir](https://flat.badgen.net/https/ucsc-ci.com/api/v4/projects/3/jobs/artifacts/issues/5183-cwl-badge/raw/badges/initial_work_dir.json%3Fjob=cwl_badge)
![inline_javascript](https://flat.badgen.net/https/ucsc-ci.com/api/v4/projects/3/jobs/artifacts/issues/5183-cwl-badge/raw/badges/inline_javascript.json%3Fjob=cwl_badge)
![inplace_update](https://flat.badgen.net/https/ucsc-ci.com/api/v4/projects/3/jobs/artifacts/issues/5183-cwl-badge/raw/badges/inplace_update.json%3Fjob=cwl_badge)
![input_object_requirements](https://flat.badgen.net/https/ucsc-ci.com/api/v4/projects/3/jobs/artifacts/issues/5183-cwl-badge/raw/badges/input_object_requirements.json%3Fjob=cwl_badge)
![json_schema_invalid](https://flat.badgen.net/https/ucsc-ci.com/api/v4/projects/3/jobs/artifacts/issues/5183-cwl-badge/raw/badges/json_schema_invalid.json%3Fjob=cwl_badge)
![load_listing](https://flat.badgen.net/https/ucsc-ci.com/api/v4/projects/3/jobs/artifacts/issues/5183-cwl-badge/raw/badges/load_listing.json%3Fjob=cwl_badge)
![multiple_input](https://flat.badgen.net/https/ucsc-ci.com/api/v4/projects/3/jobs/artifacts/issues/5183-cwl-badge/raw/badges/multiple_input.json%3Fjob=cwl_badge)
![networkaccess](https://flat.badgen.net/https/ucsc-ci.com/api/v4/projects/3/jobs/artifacts/issues/5183-cwl-badge/raw/badges/networkaccess.json%3Fjob=cwl_badge)
![required](https://flat.badgen.net/https/ucsc-ci.com/api/v4/projects/3/jobs/artifacts/issues/5183-cwl-badge/raw/badges/required.json%3Fjob=cwl_badge)
![resource](https://flat.badgen.net/https/ucsc-ci.com/api/v4/projects/3/jobs/artifacts/issues/5183-cwl-badge/raw/badges/resource.json%3Fjob=cwl_badge)
![scatter](https://flat.badgen.net/https/ucsc-ci.com/api/v4/projects/3/jobs/artifacts/issues/5183-cwl-badge/raw/badges/scatter.json%3Fjob=cwl_badge)
![schema_def](https://flat.badgen.net/https/ucsc-ci.com/api/v4/projects/3/jobs/artifacts/issues/5183-cwl-badge/raw/badges/schema_def.json%3Fjob=cwl_badge)
![step_input](https://flat.badgen.net/https/ucsc-ci.com/api/v4/projects/3/jobs/artifacts/issues/5183-cwl-badge/raw/badges/step_input.json%3Fjob=cwl_badge)
![timelimit](https://flat.badgen.net/https/ucsc-ci.com/api/v4/projects/3/jobs/artifacts/issues/5183-cwl-badge/raw/badges/timelimit.json%3Fjob=cwl_badge)
![workflow](https://flat.badgen.net/https/ucsc-ci.com/api/v4/projects/3/jobs/artifacts/issues/5183-cwl-badge/raw/badges/workflow.json%3Fjob=cwl_badge)
![work_reuse](https://flat.badgen.net/https/ucsc-ci.com/api/v4/projects/3/jobs/artifacts/issues/5183-cwl-badge/raw/badges/work_reuse.json%3Fjob=cwl_badge)
