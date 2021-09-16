.. _workflowExecutionServiceOverview:

Workflow Execution Service (WES)
================================

The GA4GH Workflow Execution Service (WES) is a standardized API for submitting and monitoring workflows.
Toil has experimental support for setting up a WES server and executing CWL, WDL, and Toil workflows using the WES API.
More information about the WES API specification can be found here_.

.. _here: https://ga4gh.github.io/workflow-execution-service-schemas/docs/

To get started with the Toil WES server, make sure that the ``server`` extra (:ref:`extras`) is installed.

.. _WESUsageOverview:

Preparing your WES environment
------------------------------

The WES server requires Celery_ to distribute and execute workflows. To set up Celery:

#. Start RabbitMQ, which is the broker between the WES server and Celery workers::

    docker run -d --name wes-rabbitmq -p 5672:5672 rabbitmq:3.9.5

#. Start Celery workers::

    celery -A toil.server.celery_app worker --detach --loglevel=INFO


.. _Celery: https://docs.celeryproject.org/en/stable/getting-started/introduction.html

Starting a WES server
---------------------

To start a WES server on the default port 8080, run the Toil command::

    $ toil server

The WES API will be hosted on the following URL::

    http://localhost:8080/ga4gh/wes/v1

To use another port, e.g.: 3000, you can specify the ``--port`` argument::

    $ toil server --port 3000

There are many other command line options. Help information can be found by using this command::

    $ toil server --help

Below is a detailed summary of all available options:


--debug
            Enable debug mode.
--host HOST
            The host interface that the Toil server binds on. (default: "127.0.0.1").
--port PORT
            The port that the Toil server listens on. (default: 8080).
--swagger_ui
            Enable the swagger UI on the ``ga4gh/wes/v1/ui`` endpoint. (default: False).
--cors
            Enable Cross Origin Resource Sharing (CORS). This should only be turned on if the server is intended to be
            used by a website or domain. (default: False).
--cors_origins ORIGIN
            Ignored if ``--cors`` is False. This sets the allowed origins for CORS. For details about CORS and its
            security risks, see the `GA4GH docs on CORS`_. (default: "*").
--workers WORKERS
            Ignored if debug mode is on. The number of worker processes launched by the production WSGI server.
            (default: 2).
--opt ENGINE_OPTION
            Specify the default parameters to be sent to the workflow engine for each run.  Accepts multiple values.

            Example: ``toil server --opt=--logLevel=CRITICAL --opt=--workDir=/tmp``.

.. _GA4GH docs on CORS: https://w3id.org/ga4gh/product-approval-support/cors


Running on EC2
--------------

To run the server on a Toil leader instance on EC2:

#. Launch a Toil cluster with the ``toil launch-cluster`` command with the AWS provisioner

#. The Toil server requires an additional TCP port to open for HTTP(S) access. To add this inbound rule, open the AWS
   console and go to the ``Security Groups`` section.  For example, on us-west2, this address would accessible at: ::

    https://us-west-2.console.aws.amazon.com/ec2/v2/home?region=us-west-2#SecurityGroups

#. Next, locate the security group created along with your cluster. It should be the same name as the cluster.

#. Click on "Edit inbound rules". Then, add a rule to allow "0.0.0.0/0" (anyone) to access port 8080 (or the port where
   the server will listen on), and click "Save rules".

    .. image:: wes_ec2_1.png
    .. image:: wes_ec2_2.png

#. Now, you can SSH into your cluster and run the WES server with ``toil server`` on the Toil appliance.

.. note::
    To run the server in the background, you can run "``nohup toil server &``".


.. _WESEndpointsOverview:

WES API Endpoints
-----------------

As defined by the GA4GH WES API specification, the following endpoints with base path ``ga4gh/wes/v1/`` are supported
by Toil:

+--------------------------------+--------------------------------------------------------+
| GET /service-info              | Get information about the Workflow Execution Service.  |
+--------------------------------+--------------------------------------------------------+
| GET /runs                      | List the workflow runs.                                |
+--------------------------------+--------------------------------------------------------+
| POST /runs                     | Run a workflow. This endpoint creates a new workflow   |
|                                | run and returns a ``run_id`` to monitor its progress.  |
+--------------------------------+--------------------------------------------------------+
| GET /runs/{run_id}             | Get detailed info about a workflow run.                |
+--------------------------------+--------------------------------------------------------+
| POST /runs/{run_id}/cancel     | Cancel a running workflow.                             |
+--------------------------------+--------------------------------------------------------+
| GET /runs/{run_id}/status      | Get the status (overall state) of a workflow run.      |
+--------------------------------+--------------------------------------------------------+

.. _WESSubmitWorkflow:

Submitting a Workflow
---------------------

Now that the WES API is up and running, we can submit and monitor workflows remotely using the WES API endpoints. A
workflow can be submitted for execution using the ``POST /runs`` endpoint.

As a quick example, we can submit the example CWL workflow from :ref:`cwlquickstart` to our WES API using cURL::

    $ curl --location --request POST 'http://localhost:8080/ga4gh/wes/v1/runs' \
        --form 'workflow_url="example.cwl"' \
        --form 'workflow_type="cwl"' \
        --form 'workflow_type_version="v1.0"' \
        --form 'workflow_params="{\"message\": \"Hello world!\"}"' \
        --form 'workflow_attachment=@"./toil_test_files/example.cwl"'
    {
      "run_id": "4deb8beb24894e9eb7c74b0f010305d1"
    }


If the workflow is submitted successfully, a JSON object containing a ``run_id`` will be returned. The ``run_id`` is a
unique identifier of your requested workflow, which can be used to monitor or cancel the run.


There are a few required parameters that have to be set for all workflow submissions, which are the following:

+---------------------------+-------------------------------------------------------------+
| workflow_url              | The URL of the workflow to run. This can refer to a file    |
|                           | from ``workflow_attachment``.                               |
+---------------------------+-------------------------------------------------------------+
| workflow_type             | The type of workflow language. Toil currently supports one  |
|                           | of the following: ``"CWL"``, ``"WDL"``, or ``"py"``. To run |
|                           | a Toil script, set this to ``"py"``.                        |
+---------------------------+-------------------------------------------------------------+
| workflow_type_version     | The version of the workflow language. Supported versions    |
|                           | can be found by accessing the ``GET /service-info``         |
|                           | endpoint of your WES server.                                |
+---------------------------+-------------------------------------------------------------+
| workflow_params           | A JSON object that specifies the inputs of the workflow.    |
+---------------------------+-------------------------------------------------------------+

Additionally, the following optional parameters are also available:

+--------------------------------+--------------------------------------------------------+
| workflow_attachment            | A list of files associated with the workflow run.      |
+--------------------------------+--------------------------------------------------------+
| workflow_engine_parameters     | A JSON key-value map of workflow engine parameters     |
|                                | to send to the runner.                                 |
|                                |                                                        |
|                                | Example:                                               |
|                                | ``{"--logLevel": "INFO", "--workDir": "/tmp/"}``       |
+--------------------------------+--------------------------------------------------------+
| tags                           | A JSON key-value map of metadata associated with the   |
|                                | workflow.                                              |
+--------------------------------+--------------------------------------------------------+


For more details about these parameters, refer to the `Run Workflow section`_ in the WES API spec.

.. _`Run Workflow section`: https://ga4gh.github.io/workflow-execution-service-schemas/docs/#operation/RunWorkflow


Upload multiple files
^^^^^^^^^^^^^^^^^^^^^

Looking at the body of the request of the previous example, note that the ``workflow_url`` is a relative URL that refers
to the ``example.cwl`` file uploaded from the local path ``./toil_test_files/example.cwl``.

To specify the file name (or subdirectory) of the remote destination file, set the ``filename`` field in the
``Content-Disposition`` header. You could also upload more than one file by providing the ``workflow_attachment``
parameter multiple times with different files.

This can be shown by the following example::

    $ curl --location --request POST 'http://localhost:8080/ga4gh/wes/v1/runs' \
        --form 'workflow_url="example.cwl"' \
        --form 'workflow_type="cwl"' \
        --form 'workflow_type_version="v1.0"' \
        --form 'workflow_params="{\"message\": \"Hello world!\"}"' \
        --form 'workflow_attachment=@"./toil_test_files/example.cwl"' \
        --form 'workflow_attachment=@"./toil_test_files/2.fasta";filename=inputs/test.fasta' \
        --form 'workflow_attachment=@"./toil_test_files/2.fastq";filename=inputs/test.fastq'

On the server, the execution directory would have the following structure from the above request::

    execution/
    ├── example.cwl
    └── inputs/
        ├── test.fasta
        └── test.fastq


.. _WESMonitoring:

Monitoring a Workflow
---------------------

With the ``run_id`` returned when submitting the workflow, we can check the status or get the full logs of the workflow
run.

Checking the state
^^^^^^^^^^^^^^^^^^

The ``GET /runs/{run_id}/status`` endpoint can be used to get a simple result with the overall state of your run::

    $ curl http://localhost:8080/ga4gh/wes/v1/runs/4deb8beb24894e9eb7c74b0f010305d1/status
    {
      "run_id": "4deb8beb24894e9eb7c74b0f010305d1",
      "state": "RUNNING"
    }


The possible states here are: ``QUEUED``, ``INITIALIZING``, ``RUNNING``, ``COMPLETE``, ``EXECUTOR_ERROR``,
``SYSTEM_ERROR``, ``CANCELING``, and ``CANCELED``.

Getting the full logs
^^^^^^^^^^^^^^^^^^^^^

To get the detailed information about a workflow run, use the ``GET /runs/{run_id}`` endpoint::

    $ curl http://localhost:8080/ga4gh/wes/v1/runs/4deb8beb24894e9eb7c74b0f010305d1
    {
      "run_id": "4deb8beb24894e9eb7c74b0f010305d1",
      "request": {
        "workflow_attachment": [
          "example.cwl"
        ],
        "workflow_url": "example.cwl",
        "workflow_type": "cwl",
        "workflow_type_version": "v1.0",
        "workflow_params": {
          "message": "Hello world!"
        }
      },
      "state": "RUNNING",
      "run_log": {
        "cmd": [
          "toil-cwl-runner --outdir=/home/toil/workflows/4deb8beb24894e9eb7c74b0f010305d1/outputs --jobStore=file:/home/toil/workflows/4deb8beb24894e9eb7c74b0f010305d1/toil_job_store /home/toil/workflows/4deb8beb24894e9eb7c74b0f010305d1/execution/example.cwl /home/workflows/4deb8beb24894e9eb7c74b0f010305d1/execution/wes_inputs.json"
        ],
        "start_time": "2021-08-30T17:35:50Z",
        "end_time": null,
        "stdout": null,
        "stderr": null,
        "exit_code": null
      },
      "task_logs": [],
      "outputs": {}
    }


Canceling a run
^^^^^^^^^^^^^^^

To cancel a workflow run, use the ``POST /runs/{run_id}/cancel`` endpoint::

    $ curl --location --request POST 'http://localhost:8080/ga4gh/wes/v1/runs/4deb8beb24894e9eb7c74b0f010305d1/cancel'
    {
      "run_id": "4deb8beb24894e9eb7c74b0f010305d1"
    }

