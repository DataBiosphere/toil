.. _conformanceWdl:

WDL Conformance Testing
=======================

The Toil team maintains a set of `WDL Conformance Tests`_. Much like the 
`CWL Conformance Tests`_ for CWL, the WDL Conformance Tests are useful for
determining if a WDL implementation actually follows the `WDL specification`_.

.. _`WDL Conformance Tests`: https://github.com/DataBiosphere/wdl-conformance-tests
.. _`CWL Conformance Tests`: https://github.com/common-workflow-language/cwl-v1.2/blob/main/CONFORMANCE_TESTS.md
.. _`WDL specification`: https://github.com/openwdl/wdl/blob/cb875867d86f868fa08f6eb2be179a50097ba440/versions/1.1/SPEC.md

The WDL Conformance Tests include a runner harness that is able to test
``toil-wdl-runner``, as well as Cromwell and MiniWDL, and supports testing
conformance with the 1.1, 1.0, and ``draft-2`` versions of WDL.

If you would like to evaluate Toil's WDL conformance for yourself, first make
sure that you have ``toil-wdl-runner`` installed. It comes with the ``[wdl]``
extra; see :ref:`extras`.

Then, you can check out the test repository::

    $ git clone https://github.com/DataBiosphere/wdl-conformance-tests
    $ cd wdl-conformance-tests

Most tests will need a Docker daemon available, so make sure yours is working
properly::

    $ docker info
    $ docker run --rm docker/whalesay cowsay "Docker is working"

Then, you can test ``toil-wdl-runner`` against a particular WDL spec version,
say 1.1::

    $ python3 run.py --runner toil-wdl-runner --versions 1.1

For any failed tests, the test number and the log of the failing test will be
reported.

After the tests run, you can clean up intermediate files with::

    $ make clean

For more options, see::

    $ python3 run.py --help

Or, consult the `conformance test documentation`_.

.. _`conformance test documentation`: https://github.com/DataBiosphere/wdl-conformance-tests/tree/master#wdl-workflow-description-language-spec-conformance-tests


