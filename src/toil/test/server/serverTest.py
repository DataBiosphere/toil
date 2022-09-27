# Copyright (C) 2015-2021 Regents of the University of California
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import json
import logging
import os
import textwrap
import time
import unittest
import uuid
import zipfile
from abc import abstractmethod
from io import BytesIO
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

try:
    from flask import Flask
    from flask.testing import FlaskClient
    from werkzeug.test import TestResponse
except ImportError:
    # We need to let pytest collect tests form this file even if the server
    # extra wasn't installed. We'll then skip them all.
    pass

from toil.test import ToilTest, needs_aws_s3, needs_celery_broker, needs_server

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

@needs_server
class ToilServerUtilsTest(ToilTest):
    """
    Tests for the utility functions used by the Toil server.
    """

    def test_workflow_canceling_recovery(self):
        """
        Make sure that a workflow in CANCELING state will be recovered to a
        terminal state eventually even if the workflow runner Celery task goes
        away without flipping the state.
        """

        from toil.server.utils import (MemoryStateStore,
                                       WorkflowStateMachine,
                                       WorkflowStateStore)

        store = WorkflowStateStore(MemoryStateStore(), "test-workflow")

        state_machine = WorkflowStateMachine(store)

        # Cancel a workflow
        state_machine.send_cancel()
        # Make sure it worked.
        self.assertEqual(state_machine.get_current_state(), "CANCELING")

        # Back-date the time of cancelation to something really old
        store.set("cancel_time", "2011-11-04 00:05:23.283")

        # Make sure it is now CANCELED due to timeout
        self.assertEqual(state_machine.get_current_state(), "CANCELED")

class hidden:
    # Hide abstract tests from the test loader

    @needs_server
    class AbstractStateStoreTest(ToilTest):
        """
        Basic tests for state stores.
        """

        from toil.server.utils import AbstractStateStore

        @abstractmethod
        def get_state_store(self) -> AbstractStateStore:
            """
            Make a state store to test, on a single fixed URL.
            """

            raise NotImplementedError()


        def test_state_store(self) -> None:
            """
            Make sure that the state store under test can store and load keys.
            """

            store = self.get_state_store()

            # Should start None
            self.assertEqual(store.get('id1', 'key1'), None)

            # Should hold a value
            store.set('id1', 'key1', 'value1')
            self.assertEqual(store.get('id1', 'key1'), 'value1')

            # Should distinguish by ID and key
            self.assertEqual(store.get('id2', 'key1'), None)
            self.assertEqual(store.get('id1', 'key2'), None)

            store.set('id2', 'key1', 'value2')
            store.set('id1', 'key2', 'value3')
            self.assertEqual(store.get('id1', 'key1'), 'value1')
            self.assertEqual(store.get('id2', 'key1'), 'value2')
            self.assertEqual(store.get('id1', 'key2'), 'value3')

            # Should allow replacement
            store.set('id1', 'key1', 'value4')
            self.assertEqual(store.get('id1', 'key1'), 'value4')
            self.assertEqual(store.get('id2', 'key1'), 'value2')
            self.assertEqual(store.get('id1', 'key2'), 'value3')

            # Should show up in another state store
            store2 = self.get_state_store()
            self.assertEqual(store2.get('id1', 'key1'), 'value4')
            self.assertEqual(store2.get('id2', 'key1'), 'value2')
            self.assertEqual(store2.get('id1', 'key2'), 'value3')

            # Should allow clearing
            store.set('id1', 'key1', None)
            self.assertEqual(store.get('id1', 'key1'), None)
            self.assertEqual(store.get('id2', 'key1'), 'value2')
            self.assertEqual(store.get('id1', 'key2'), 'value3')

            store.set('id2', 'key1', None)
            store.set('id1', 'key2', None)
            self.assertEqual(store.get('id1', 'key1'), None)
            self.assertEqual(store.get('id2', 'key1'), None)
            self.assertEqual(store.get('id1', 'key2'), None)

class FileStateStoreTest(hidden.AbstractStateStoreTest):
    """
    Test file-based state storage.
    """

    from toil.server.utils import AbstractStateStore

    def setUp(self) -> None:
        super().setUp()
        self.state_store_dir = self._createTempDir()

    def get_state_store(self) -> AbstractStateStore:
        """
        Make a state store to test, on a single fixed local path.
        """

        from toil.server.utils import FileStateStore

        return FileStateStore(self.state_store_dir)

class FileStateStoreURLTest(hidden.AbstractStateStoreTest):
    """
    Test file-based state storage using URLs instead of local paths.
    """

    from toil.server.utils import AbstractStateStore

    def setUp(self) -> None:
        super().setUp()
        self.state_store_dir = 'file://' + self._createTempDir()

    def get_state_store(self) -> AbstractStateStore:
        """
        Make a state store to test, on a single fixed URL.
        """

        from toil.server.utils import FileStateStore

        return FileStateStore(self.state_store_dir)

@needs_aws_s3
class BucketUsingTest(ToilTest):
    """
    Base class for tests that need a bucket.
    """

    try:
        # We need the class to be evaluateable without the AWS modules, if not
        # runnable
        from mypy_boto3_s3 import S3ServiceResource
        from mypy_boto3_s3.service_resource import Bucket
    except ImportError:
        pass


    region: Optional[str]
    s3_resource: Optional['S3ServiceResource']
    bucket: Optional['Bucket']
    bucket_name: Optional[str]

    @classmethod
    def setUpClass(cls) -> None:
        """
        Set up the class with a single pre-existing AWS bucket for all tests.
        """
        super().setUpClass()

        from toil.lib.aws import get_current_aws_region, session
        from toil.lib.aws.utils import create_s3_bucket

        cls.region = get_current_aws_region()
        cls.s3_resource = session.resource("s3", region_name=cls.region)

        cls.bucket_name = f"toil-test-{uuid.uuid4()}"
        cls.bucket = create_s3_bucket(cls.s3_resource, cls.bucket_name, cls.region)
        cls.bucket.wait_until_exists()

    @classmethod
    def tearDownClass(cls) -> None:
        from toil.lib.aws.utils import delete_s3_bucket
        if cls.bucket_name:
            delete_s3_bucket(cls.s3_resource, cls.bucket_name, cls.region)
        super().tearDownClass()

class AWSStateStoreTest(hidden.AbstractStateStoreTest, BucketUsingTest):
    """
    Test AWS-based state storage.
    """

    from toil.server.utils import AbstractStateStore

    bucket_path = "prefix/of/keys"

    def get_state_store(self) -> AbstractStateStore:
        """
        Make a state store to test, on a single fixed URL.
        """

        from toil.server.utils import S3StateStore

        return S3StateStore('s3://' + self.bucket_name + '/' + self.bucket_path)

    def test_state_store_paths(self) -> None:
        """
        Make sure that the S3 state store puts things in the right places.

        We don't *really* care about the exact internal structure, but we do
        care about actually being under the path we are supposed to use.
        """

        from toil.lib.aws.utils import get_object_for_url

        store = self.get_state_store()

        # Should hold a value
        store.set('testid', 'testkey', 'testvalue')
        self.assertEqual(store.get('testid', 'testkey'), 'testvalue')

        expected_url = urlparse('s3://' + self.bucket_name + '/' +
            os.path.join(self.bucket_path, 'testid', 'testkey'))

        obj = get_object_for_url(expected_url, True)
        self.assertEqual(obj.content_length, len('testvalue'))




@needs_server
class AbstractToilWESServerTest(ToilTest):
    """
    Class for server tests that provides a self.app in testing mode.
    """

    def __init__(self, *args, **kwargs):
        """
        Set up default settings for test classes based on this one.
        """
        super().__init__(*args, **kwargs)

        # Default to the local testing task runner instead of Celery for when
        # we run workflows.
        self._server_args = ["--bypass_celery"]

    def setUp(self) -> None:
        super().setUp()
        self.temp_dir = self._createTempDir()

        from toil.server.app import create_app, parser_with_server_options
        parser = parser_with_server_options()
        args = parser.parse_args(self._server_args + ["--work_dir", os.path.join(self.temp_dir, "workflows")])

        # Make the FlaskApp
        server_app = create_app(args)

        # Fish out the actual Flask
        self.app: Flask = server_app.app
        self.app.testing = True

        self.example_cwl = textwrap.dedent("""
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
            """)

        self.slow_cwl = textwrap.dedent("""
            cwlVersion: v1.0
            class: CommandLineTool
            baseCommand: sleep
            stdout: output.txt
            inputs:
              delay:
                type: string
                inputBinding:
                  position: 1
            outputs:
              output:
                type: stdout
            """)

    def tearDown(self) -> None:
        super().tearDown()

    def _fetch_run_log(self, client: "FlaskClient", run_id: str) -> "TestResponse":
        """
        Fetch the run log for a given workflow.
        """
        rv = client.get(f"/ga4gh/wes/v1/runs/{run_id}")
        self.assertEqual(rv.status_code, 200)
        self.assertTrue(rv.is_json)
        return rv

    def _check_successful_log(self, client: "FlaskClient", run_id: str) -> None:
        """
        Make sure the run log for the given run is generated correctly.
        The workflow should succeed, it should have some tasks, and they should have all succeeded.
        """
        rv = self._fetch_run_log(client, run_id)
        logger.debug('Log info: %s', rv.json)
        self.assertIn("run_log", rv.json)
        run_log = rv.json.get("run_log")
        self.assertEqual(type(run_log), dict)
        if "exit_code" in run_log:
            # The workflow succeeded if it has an exit code
            self.assertEqual(run_log["exit_code"], 0)
        # The workflow is complete
        self.assertEqual(rv.json.get("state"), "COMPLETE")
        task_logs = rv.json.get("task_logs")
        # There are tasks reported
        self.assertEqual(type(task_logs), list)
        self.assertGreater(len(task_logs), 0)
        for task_log in task_logs:
            # All the tasks succeeded
            self.assertEqual(type(task_log), dict)
            self.assertEqual(task_log.get("exit_code"), 0)

    def _report_log(self, client: "FlaskClient", run_id: str) -> None:
        """
        Report the log for the given workflow run.
        """
        rv = self._fetch_run_log(client, run_id)
        self.assertIn("run_log", rv.json)
        run_log = rv.json.get("run_log")
        self.assertEqual(type(run_log), dict)
        self.assertIn("stdout", run_log)
        stdout = run_log.get("stdout")
        self.assertEqual(type(stdout), str)
        self.assertIn("stderr", run_log)
        stderr = run_log.get("stderr")
        self.assertEqual(type(stderr), str)
        logger.info("Got stdout %s and stderr %s", stdout, stderr)
        self._report_absolute_url(client, stdout)
        self._report_absolute_url(client, stderr)

    def _report_absolute_url(self, client: "FlaskClient", url: str):
        """
        Take a URL that has a hostname and port, relativize it to the test
        Flask client, and get its contents and log them.
        """

        breakpoint = url.find("/toil/")
        if breakpoint == -1:
            raise RuntimeError("Could not find place to break URL: " + url)
        url = url[breakpoint:]
        logger.info("Fetch %s", url)
        rv = client.get(url)
        self.assertEqual(rv.status_code, 200)
        logger.info("Got %s:\n%s", url, rv.data.decode('utf-8'))

    def _start_slow_workflow(self, client: "FlaskClient") -> str:
        """
        Start a slow workflow and return its ID.
        """
        rv = client.post("/ga4gh/wes/v1/runs", data={
            "workflow_url": "slow.cwl",
            "workflow_type": "CWL",
            "workflow_type_version": "v1.0",
            "workflow_params": json.dumps({"delay": "5"}),
            "workflow_attachment": [
                (BytesIO(self.slow_cwl.encode()), "slow.cwl"),
            ],
        })
        # workflow is submitted successfully
        self.assertEqual(rv.status_code, 200)
        self.assertTrue(rv.is_json)
        run_id = rv.json.get("run_id")
        self.assertIsNotNone(run_id)

        return run_id

    def _poll_status(self, client: "FlaskClient", run_id: str) -> str:
        """
        Get the status of the given workflow.
        """

        rv = client.get(f"/ga4gh/wes/v1/runs/{run_id}/status")
        self.assertEqual(rv.status_code, 200)
        self.assertTrue(rv.is_json)
        self.assertIn("run_id", rv.json)
        self.assertEqual(rv.json.get("run_id"), run_id)
        self.assertIn("state", rv.json)
        state = rv.json.get("state")
        self.assertIn(state, ["UNKNOWN", "QUEUED", "INITIALIZING", "RUNNING",
                              "PAUSED", "COMPLETE", "EXECUTOR_ERROR", "SYSTEM_ERROR",
                              "CANCELED", "CANCELING"])
        return state

    def _cancel_workflow(self, client: "FlaskClient", run_id: str) -> None:
        rv = client.post(f"/ga4gh/wes/v1/runs/{run_id}/cancel")
        self.assertEqual(rv.status_code, 200)

    def _wait_for_status(self, client: "FlaskClient", run_id: str, target_status: str) -> None:
        """
        Wait for the given workflow run to reach the given state. If it reaches
        a different terminal state, raise an exception.
        """

        while True:
            state = self._poll_status(client, run_id)
            if state == target_status:
                # We're done!
                logger.info("Workflow reached state %s", state)
                return
            if state in ["EXECUTOR_ERROR", "SYSTEM_ERROR", "CANCELED", "COMPLETE"]:
                logger.error("Workflow run reached unexpected terminal state %s", state)
                self._report_log(client, run_id)
                raise RuntimeError("Workflow is in unexpected state " + state)
            logger.info("Waiting on workflow in state %s", state)
            time.sleep(2)

    def _wait_for_success(self, client: "FlaskClient", run_id: str) -> None:
        """
        Wait for the given workflow run to succeed. If it fails, raise an exception.
        """
        self._wait_for_status(client, run_id, "COMPLETE")
        self._check_successful_log(client, run_id)


class ToilWESServerBenchTest(AbstractToilWESServerTest):
    """
    Tests for Toil's Workflow Execution Service API that don't run workflows.
    """

    def test_home(self) -> None:
        """ Test the homepage endpoint."""
        with self.app.test_client() as client:
            rv = client.get("/")
        self.assertEqual(rv.status_code, 302)

    def test_health(self) -> None:
        """ Test the health check endpoint."""
        with self.app.test_client() as client:
            rv = client.get("/engine/v1/status")
        self.assertEqual(rv.status_code, 200)

    def test_get_service_info(self) -> None:
        """ Test the GET /service-info endpoint."""
        with self.app.test_client() as client:
            rv = client.get("/ga4gh/wes/v1/service-info")
        self.assertEqual(rv.status_code, 200)
        service_info = json.loads(rv.data)

        self.assertIn("version", service_info)
        self.assertIn("workflow_type_versions", service_info)
        self.assertIn("supported_wes_versions", service_info)
        self.assertIn("supported_filesystem_protocols", service_info)
        self.assertIn("workflow_engine_versions", service_info)
        engine_versions = service_info["workflow_engine_versions"]
        self.assertIn("toil", engine_versions)
        self.assertEqual(type(engine_versions["toil"]), str)
        self.assertIn("default_workflow_engine_parameters", service_info)
        self.assertIn("system_state_counts", service_info)
        self.assertIn("tags", service_info)

class ToilWESServerWorkflowTest(AbstractToilWESServerTest):
    """
    Tests of the WES server running workflows.
    """

    def run_zip_workflow(self, zip_path: str, include_message: bool = True, include_params: bool = True) -> None:
        """
        We have several zip file tests; this submits a zip file and makes sure it ran OK.

        If include_message is set to False, don't send a "message" argument in  workflow_params.
        If include_params is also set to False, don't send workflow_params at all.
        """
        self.assertTrue(os.path.exists(zip_path))

        # Set up what we will POST to start the workflow.
        post_data = {
            "workflow_url": "file://" + zip_path,
            "workflow_type": "CWL",
            "workflow_type_version": "v1.0"
        }
        if include_params or include_message:
            # We need workflow_params too
            post_data["workflow_params"] = json.dumps({"message": "Hello, world!"} if include_message else {})
        with self.app.test_client() as client:
            rv = client.post("/ga4gh/wes/v1/runs", data=post_data)
            # workflow is submitted successfully
            self.assertEqual(rv.status_code, 200)
            self.assertTrue(rv.is_json)
            run_id = rv.json.get("run_id")
            self.assertIsNotNone(run_id)

            # Check status
            self._wait_for_success(client, run_id)

            # TODO: Make sure that the correct message was output!

    def test_run_workflow_relative_url_no_attachments_fails(self) -> None:
        """Test run example CWL workflow from relative workflow URL but with no attachments."""
        with self.app.test_client() as client:
            rv = client.post("/ga4gh/wes/v1/runs", data={
                "workflow_url": "example.cwl",
                "workflow_type": "CWL",
                "workflow_type_version": "v1.0",
                "workflow_params": "{}"
            })
            self.assertEqual(rv.status_code, 400)
            self.assertTrue(rv.is_json)
            self.assertEqual(rv.json.get("msg"), "Relative 'workflow_url' but missing 'workflow_attachment'")

    def test_run_workflow_relative_url(self) -> None:
        """Test run example CWL workflow from relative workflow URL."""
        with self.app.test_client() as client:
            rv = client.post("/ga4gh/wes/v1/runs", data={
                "workflow_url": "example.cwl",
                "workflow_type": "CWL",
                "workflow_type_version": "v1.0",
                "workflow_params": json.dumps({"message": "Hello, world!"}),
                "workflow_attachment": [
                    (BytesIO(self.example_cwl.encode()), "example.cwl"),
                ],
            })
            # workflow is submitted successfully
            self.assertEqual(rv.status_code, 200)
            self.assertTrue(rv.is_json)
            run_id = rv.json.get("run_id")
            self.assertIsNotNone(run_id)

            # Check status
            self._wait_for_success(client, run_id)

    def test_run_workflow_https_url(self) -> None:
        """Test run example CWL workflow from the Internet."""
        with self.app.test_client() as client:
            rv = client.post("/ga4gh/wes/v1/runs", data={
                "workflow_url": "https://raw.githubusercontent.com/DataBiosphere/toil/releases/5.4.x/src/toil"
                                "/test/docs/scripts/cwlExampleFiles/hello.cwl",
                "workflow_type": "CWL",
                "workflow_type_version": "v1.0",
                "workflow_params": json.dumps({"message": "Hello, world!"}),
            })
            # workflow is submitted successfully
            self.assertEqual(rv.status_code, 200)
            self.assertTrue(rv.is_json)
            run_id = rv.json.get("run_id")
            self.assertIsNotNone(run_id)

            # Check status
            self._wait_for_success(client, run_id)

    def test_run_workflow_single_file_zip(self) -> None:
        """Test run example CWL workflow from single-file ZIP."""
        workdir = self._createTempDir()
        zip_path = os.path.abspath(os.path.join(workdir, 'workflow.zip'))
        with zipfile.ZipFile(zip_path, 'w') as zip_file:
            zip_file.writestr('example.cwl', self.example_cwl)
        self.run_zip_workflow(zip_path)

    def test_run_workflow_multi_file_zip(self) -> None:
        """Test run example CWL workflow from multi-file ZIP."""
        workdir = self._createTempDir()
        zip_path = os.path.abspath(os.path.join(workdir, 'workflow.zip'))
        with zipfile.ZipFile(zip_path, 'w') as zip_file:
            zip_file.writestr('main.cwl', self.example_cwl)
            zip_file.writestr('distraction.cwl', "Don't mind me")
        self.run_zip_workflow(zip_path)

    def test_run_workflow_manifest_zip(self) -> None:
        """Test run example CWL workflow from ZIP with manifest."""
        workdir = self._createTempDir()
        zip_path = os.path.abspath(os.path.join(workdir, 'workflow.zip'))
        with zipfile.ZipFile(zip_path, 'w') as zip_file:
            zip_file.writestr('actual.cwl', self.example_cwl)
            zip_file.writestr('distraction.cwl', self.example_cwl)
            zip_file.writestr('MANIFEST.json', json.dumps({"mainWorkflowURL": "actual.cwl"}))
        self.run_zip_workflow(zip_path)


    def test_run_workflow_inputs_zip(self) -> None:
        """Test run example CWL workflow from ZIP without manifest but with inputs."""
        workdir = self._createTempDir()
        zip_path = os.path.abspath(os.path.join(workdir, 'workflow.zip'))
        with zipfile.ZipFile(zip_path, 'w') as zip_file:
            zip_file.writestr('main.cwl', self.example_cwl)
            zip_file.writestr('inputs.json', json.dumps({"message": "Hello, world!"}))
        self.run_zip_workflow(zip_path, include_message=False)

    def test_run_workflow_manifest_and_inputs_zip(self) -> None:
        """Test run example CWL workflow from ZIP with manifest and inputs."""
        workdir = self._createTempDir()
        zip_path = os.path.abspath(os.path.join(workdir, 'workflow.zip'))
        with zipfile.ZipFile(zip_path, 'w') as zip_file:
            zip_file.writestr('actual.cwl', self.example_cwl)
            zip_file.writestr('data.json', json.dumps({"message": "Hello, world!"}))
            zip_file.writestr('MANIFEST.json', json.dumps({"mainWorkflowURL": "actual.cwl", "inputFileURLs": ["data.json"]}))
        self.run_zip_workflow(zip_path, include_message=False)

    def test_run_workflow_no_params_zip(self) -> None:
        """Test run example CWL workflow from ZIP without workflow_params."""
        workdir = self._createTempDir()
        zip_path = os.path.abspath(os.path.join(workdir, 'workflow.zip'))
        with zipfile.ZipFile(zip_path, 'w') as zip_file:
            zip_file.writestr('main.cwl', self.example_cwl)
            zip_file.writestr('inputs.json', json.dumps({"message": "Hello, world!"}))
        # Don't even bother sending workflow_params
        self.run_zip_workflow(zip_path, include_message=False, include_params=False)

    def test_run_and_cancel_workflows(self) -> None:
        """
        Run two workflows, cancel one of them, and make sure they all exist.
        """
        with self.app.test_client() as client:
            # Start 1 workflow
            run1 = self._start_slow_workflow(client)
            time.sleep(2)
            status1 = self._poll_status(client, run1)
            self.assertIn(status1, ["QUEUED", "INITIALIZING", "RUNNING"])

            # Start another workflow
            run2 = self._start_slow_workflow(client)
            self.assertNotEqual(run1, run2)
            time.sleep(2)
            status2 = self._poll_status(client, run2)
            self.assertIn(status2, ["QUEUED", "INITIALIZING", "RUNNING"])

            # Cancel the second one
            cancel_sent = time.time()
            self._cancel_workflow(client, run2)
            time.sleep(1)
            status2 = self._poll_status(client, run2)
            self.assertIn(status2, ["CANCELING", "CANCELED"])

            # Make sure the first one still exists too
            status1 = self._poll_status(client, run1)
            self.assertIn(status1, ["QUEUED", "INITIALIZING", "RUNNING"])

            self._wait_for_status(client, run2, "CANCELED")
            cancel_complete = time.time()
            self._wait_for_success(client, run1)

            # Make sure the cancellation was relatively prompt and we didn't
            # have to go through any of the timeout codepaths
            cancel_seconds = cancel_complete - cancel_sent
            logger.info("Cancellation took %s seconds to complete", cancel_seconds)
            from toil.server.wes.tasks import WAIT_FOR_DEATH_TIMEOUT
            self.assertLess(cancel_seconds, WAIT_FOR_DEATH_TIMEOUT)

@needs_celery_broker
class ToilWESServerCeleryWorkflowTest(ToilWESServerWorkflowTest):
    """
    End-to-end workflow-running tests against Celery.
    """

    def __init__(self, *args, **kwargs):
        """
        Set the task runner back to Celery.
        """
        super().__init__(*args, **kwargs)
        self._server_args = []

@needs_celery_broker
class ToilWESServerCeleryS3StateWorkflowTest(ToilWESServerWorkflowTest, BucketUsingTest):
    """
    Test the server with Celery and state stored in S3.
    """

    def setUp(self) -> None:
        # Overwrite server args from __init__. The bucket name isn't available when __init__ runs.
        self._server_args = ["--state_store", "s3://" + self.bucket_name + "/state"]
        super().setUp()

if __name__ == "__main__":
    unittest.main()
