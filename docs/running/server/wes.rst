.. _workflowExecutionServiceOverview:

Workflow Execution Service (WES)
================================

The GA4GH Workflow Execution Service (WES) is a standardized API for submitting and monitoring workflows.
Toil has experimental support for setting up a WES server and executing CWL, WDL, and Toil workflows using the WES API.
More information about the WES API specification can be found here_.

.. _here: https://ga4gh.github.io/workflow-execution-service-schemas/docs/

To get started with the Toil WES server, make sure that the ``server`` extra (:ref:`extras`) is installed.

.. _WESPrepareEnvironment:

Preparing your WES environment
------------------------------

The WES server requires Celery_ to distribute and execute workflows. To set up Celery:

#. Start RabbitMQ, which is the broker between the WES server and Celery workers::

    docker run -d --name wes-rabbitmq -p 5672:5672 rabbitmq:3.9.5

#. Start Celery workers::

    celery -A toil.server.celery_app worker --loglevel=INFO


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

Below is a detailed summary of all server-specific options:


--debug     Enable debug mode.
--bypass_celery
            Skip sending workflows to Celery and just run them under the server.
            For testing.
--host HOST
            The host interface that the Toil server binds on. (default:
            "127.0.0.1").
--port PORT
            The port that the Toil server listens on. (default: 8080).
--swagger_ui
            If True, the swagger UI will be enabled and hosted on the
            ``{api_base_path}/ui`` endpoint. (default: False)
--cors
            Enable Cross Origin Resource Sharing (CORS). This should only be
            turned on if the server is intended to be used by a website or
            domain. (default: False).
--cors_origins CORS_ORIGIN
            Ignored if ``--cors`` is False. This sets the allowed origins for
            CORS. For details about CORS and its security risks, see the
            `GA4GH docs on CORS`_. (default: "*").
--workers WORKERS, -w WORKERS
            Ignored if ``--debug`` is True. The number of worker processes
            launched by the WSGI server. (default: 2).
--work_dir WORK_DIR
            The directory where workflows should be stored. This directory
            should be empty or only contain previous workflows. (default:
            './workflows').
--state_store STATE_STORE
            The local path or S3 URL where workflow state metadata should be
            stored. (default: in ``--work_dir``)
--opt OPT, -o OPT
            Specify the default parameters to be sent to the workflow engine for
            each run.  Options taking arguments must use = syntax. Accepts
            multiple values. Example: ``--opt=--logLevel=CRITICAL`` ``--opt=--workDir=/tmp``.
--dest_bucket_base DEST_BUCKET_BASE
            Direct CWL workflows to save output files to dynamically generated
            unique paths under the given URL. Supports AWS S3.
--wes_dialect DIALECT
            Restrict WES responses to a dialect compatible with clients that do
            not fully implement the WES standard. (default: 'standard')

.. _GA4GH docs on CORS: https://w3id.org/ga4gh/product-approval-support/cors


.. _WESRunWithDockerCompose:

Running the Server with `docker-compose`
----------------------------------------

Instead of manually setting up the server components (``toil server``, RabbitMQ, and Celery), you can use the following
``docker-compose.yml`` file to orchestrate and link them together.

Make sure to change the credentials for basic authentication by updating the
``traefik.http.middlewares.auth.basicauth.users`` label. The passwords can be generated with tools like ``htpasswd``
`like this`_. (Note that single ``$`` signs need to be replaced with ``$$`` in the yaml file).

.. _`like this`: https://doc.traefik.io/traefik/v2.0/middlewares/basicauth/#configuration-examples


When running on a different host other than ``localhost``, make sure to change the ``Host`` to your
tartget host in the ``traefik.http.routers.wes.rule`` and ``traefik.http.routers.wespublic.rule`` labels.

You can also change ``/tmp/toil-workflows`` if you want Toil workflows to live somewhere else, and create the directory
before starting the server.

In order to run workflows that require Docker, the ``docker.sock`` socket must be mounted as volume for Celery.
Additionally, the ``TOIL_WORKDIR`` directory (defaults to: ``/var/lib/toil``) and ``/var/lib/cwl`` (if running CWL
workflows with ``DockerRequirement``) should exist on the host and also be mounted as volumes.


Also make sure to run it behind a firewall; it opens up the Toil server on port 8080 to anyone who connects.

.. literalinclude:: ./docker-compose.yml
   :language: yaml
   :emphasize-lines: 13-15, 22,25, 30,33,34, 43,45


Further customization can also be made as needed. For example, if you have a domain, you can
`set up HTTPS with Let's Encrypt`_.

.. _`set up HTTPs with Let's Encrypt`: https://doc.traefik.io/traefik/user-guides/docker-compose/acme-http/

Once everything is configured, simply run ``docker-compose up`` to start the containers. Run ``docker-compose down`` to
stop and remove all containers.

.. note::
    ``docker-compose`` is not installed on the Toil appliance by default. See the following section to set up the WES
    server on a Toil cluster.


Running on a Toil cluster
-------------------------

To run the server on a Toil leader instance on EC2:

#. Launch a Toil cluster with the ``toil launch-cluster`` command with the AWS provisioner

#. SSH into your cluster with the ``--sshOption=-L8080:localhost:8080`` option to forward port ``8080``

#. Install Docker Compose by running the following commands from the `Docker docs`_::

        curl -L "https://github.com/docker/compose/releases/download/1.29.2/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
        chmod +x /usr/local/bin/docker-compose

        # check installation
        docker-compose --version

   or, install a different version of Docker Compose by changing ``"1.29.2"`` to another version.

#. Copy the ``docker-compose.yml`` file from (:ref:`WESRunWithDockerCompose`) to an empty directory, and modify the
   configuration as needed.

#. Now, run ``docker-compose up -d`` to start the WES server in detach mode on the Toil appliance.

#. To stop the server, run ``docker-compose down``.


.. _Docker docs: https://docs.docker.com/compose/install/#install-compose


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

When running the WES server with the ``docker-compose`` setup above, most endpoints (except ``GET /service-info``) will
be protected with basic authentication. Make sure to set the **Authorization** header with the correct credentials when
submitting or retrieving a workflow.


.. _WESSubmitWorkflow:

Submitting a Workflow
---------------------

Now that the WES API is up and running, we can submit and monitor workflows remotely using the WES API endpoints. A
workflow can be submitted for execution using the ``POST /runs`` endpoint.

As a quick example, we can submit the example CWL workflow from :ref:`cwlquickstart` to our WES API:

.. code-block:: yaml

    # example.cwl
    cwlVersion: v1.0
    class: CommandLineTool
    baseCommand: echo
    stdout: output.txt
    inputs:
     message:
       type: string
       inputBinding:
         position: 1
    outputs:
     output:
       type: stdout

using cURL::

    $ curl --location --request POST 'http://localhost:8080/ga4gh/wes/v1/runs' \
        --user test:test \
        --form 'workflow_url="example.cwl"' \
        --form 'workflow_type="cwl"' \
        --form 'workflow_type_version="v1.0"' \
        --form 'workflow_params="{\"message\": \"Hello world!\"}"' \
        --form 'workflow_attachment=@"./toil_test_files/example.cwl"'
    {
      "run_id": "4deb8beb24894e9eb7c74b0f010305d1"
    }


Note that the ``--user`` argument is used to attach the basic authentication credentials along with the request. Make
sure to change ``test:test`` to the username and password you configured for your WES server. Alternatively, you can
also set the **Authorization** header manually as ``"Authorization: Basic base64_encoded_auth"``.

If the workflow is submitted successfully, a JSON object containing a ``run_id`` will be returned. The ``run_id`` is a
unique identifier of your requested workflow, which can be used to monitor or cancel the run.


There are a few required parameters that have to be set for all workflow submissions, which are the following:

+---------------------------+-------------------------------------------------------------+
| workflow_url              | The URL of the workflow to run. This can refer to a file    |
|                           | from ``workflow_attachment``.                               |
+---------------------------+-------------------------------------------------------------+
| workflow_type             | The type of workflow language. Toil currently supports one  |
|                           | of the following: ``"CWL"``, ``"WDL"``, or ``"py"``. To run |
|                           | a Toil Python workflow, set this to ``"py"``.               |
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
        --user test:test \
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
    ├── inputs
    │     ├── test.fasta
    |     └── test.fastq
    └── wes_inputs.json


Specify Toil options
^^^^^^^^^^^^^^^^^^^^

To pass Toil-specific parameters to the workflow, you can include the ``workflow_engine_parameters`` parameter along
with your request.

For example, to set the logging level to ``INFO``, and change the working directory of the workflow, simply include the
following as ``workflow_engine_parameters``::

    {"--logLevel": "INFO", "--workDir": "/tmp/"}

These options would be appended at the end of existing parameters during command construction, which would override the
default parameters if provided. (Default parameters that can be passed multiple times would not be overridden).


.. _WESMonitoring:

Monitoring a Workflow
---------------------

With the ``run_id`` returned when submitting the workflow, we can check the status or get the full logs of the workflow
run.

Checking the state
^^^^^^^^^^^^^^^^^^

The ``GET /runs/{run_id}/status`` endpoint can be used to get a simple result with the overall state of your run::

    $ curl --user test:test http://localhost:8080/ga4gh/wes/v1/runs/4deb8beb24894e9eb7c74b0f010305d1/status
    {
      "run_id": "4deb8beb24894e9eb7c74b0f010305d1",
      "state": "RUNNING"
    }


The possible states here are: ``QUEUED``, ``INITIALIZING``, ``RUNNING``, ``COMPLETE``, ``EXECUTOR_ERROR``,
``SYSTEM_ERROR``, ``CANCELING``, and ``CANCELED``.

Getting the full logs
^^^^^^^^^^^^^^^^^^^^^

To get the detailed information about a workflow run, use the ``GET /runs/{run_id}`` endpoint::

    $ curl --user test:test http://localhost:8080/ga4gh/wes/v1/runs/4deb8beb24894e9eb7c74b0f010305d1
    {
      "run_id": "4deb8beb24894e9eb7c74b0f010305d1",
      "request": {
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

    $ curl --location --request POST 'http://localhost:8080/ga4gh/wes/v1/runs/4deb8beb24894e9eb7c74b0f010305d1/cancel' \
          --user test:test
    {
      "run_id": "4deb8beb24894e9eb7c74b0f010305d1"
    }

