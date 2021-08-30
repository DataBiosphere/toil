.. _workflowExecutionServiceOverview:

Workflow Execution Service (WES)
================================

The GA4GH Workflow Execution Service (WES) is a standardized API for submitting and monitoring workflows across
multiple platforms. Toil has experimental support for setting up a WES server and executing CWL, WDL, and Toil
workflows using the WES API. More information about the WES API specification can be found here_.

.. _here: https://ga4gh.github.io/workflow-execution-service-schemas/docs/


Running a WES server
--------------------

To start a WES server on the default port 8080, run the following command::

  $ toil server

The WES API will be hosted on the following URL::

  http://localhost:8080/ga4gh/wes/v1

To use another port, e.g.: 3000, you can specify the ``--port`` argument::

  $ toil server --port 3000

There are a number of command line options. Help information can be found by using this command::

  $ toil server --help

Below is a detailed summary of all available options:

``—-debug``: Enable debug mode.

``—-port``: The port that the Toil server listens on. (default: 8080).

``—-swagger_ui``: If True, the swagger UI will be enabled and hosted on the ``{API}/ui`` endpoint. (default: False).

``--cors``: Enable Cross Origin Resource Sharing (CORS). This should only be turned on if the server is intended
to be used by a website or domain. (default: False).

``--cors_origins``: Ignored if --cors is False. This sets the allowed origins for CORS. For details about CORS and
its security risks, see the `GA4GH docs on CORS`_. (default: "*")

``--workers``: Ignored if debug mode is on. The number of worker processes launched by the production WSGI server.
`2-4 workers per core`_ is recommended.

``-—opt``:

.. _2-4 workers per core: https://docs.gunicorn.org/en/stable/design.html#how-many-workers
.. _GA4GH docs on CORS: https://w3id.org/ga4gh/product-approval-support/cors


.. _WESEndpointsOverview:

WES API Endpoints
-----------------

As defined by the GA4GH WES API specification, the following endpoints are supported by Toil:

``GET /service-info``

``GET /runs``

``POST /runs``

``GET /runs/{run_id}``

``GET /runs/{run_id}/status``

``POST /runs/{run_id}/cancel``


.. _submitWorkflow:

Submit a Workflow
-----------------

A workflow can be submitted for execution using the ``POST /runs`` endpoint.

There are a few required parameters that need to be set for the body of the request, which are the following:

* ``workflow_url``
* ``workflow_type``
* ``workflow_type_version``
* and ``workflow_params``

Additionally, you can also provide the following parameters in your request:

* ``tags`` (not supported by Toil, yet)
* ``workflow_engine_parameters``


Please refer to the `WES API spec`_ for more details.

.. _`WES API spec`: https://ga4gh.github.io/workflow-execution-service-schemas/docs/#operation/RunWorkflow


For example, we can submit the example CWL workflow from :ref:`cwlquickstart` to our WES API using cURL::

    $ curl --location --request POST 'http://localhost:8080/ga4gh/wes/v1/runs' \
        --form 'workflow_url="example.cwl"' \
        --form 'workflow_type="cwl"' \
        --form 'workflow_type_version="v1.0"' \
        --form 'workflow_params="{\"message\": \"Hello world!\"}"' \
        --form 'workflow_attachment=@"./toil_test_files/example.cwl"'

Note that in this example, ``workflow_url`` is a relative URL that refers to the ``example.cwl`` file uploaded from
the relative path ``./toil_test_files/example.cwl`` as ``workflow_attachment``.

To run a workflow that requires multiple files, you can set the ``workflow_attachment`` parameter multiple times with
your different files. You could also specify the file name (and sub directory) by setting the "filename" in the
Content-Disposition header.

This can be shown by the following example::

    $ curl --location --request POST 'http://localhost:8080/ga4gh/wes/v1/runs' \
        --form 'workflow_url="example.cwl"' \
        --form 'workflow_type="cwl"' \
        --form 'workflow_type_version="v1.0"' \
        --form 'workflow_params="{\"message\": \"Hello world!\"}"' \
        --form 'workflow_attachment=@"./toil_test_files/example.cwl"' \
        --form 'workflow_attachment=@"./toil_test_files/2.fasta";filename=inputs/test.fasta' \
        --form 'workflow_attachment=@"./toil_test_files/2.fastq";filename=inputs/test.fastq'

The execution directory would have the following structure from the above request::

    execution/
    ├── example.cwl
    └── inputs/
      ├── test.fasta
      └── test.fastq

