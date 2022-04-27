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
import zipfile
from io import BytesIO
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

try:
    from flask import Flask
    from flask.testing import FlaskClient
except ImportError:
    # We need to let pytest collect tests form this file even if the server
    # extra wasn't installed. We'll then skip them all.
    pass

from toil.test import ToilTest, needs_server, needs_celery_broker

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
        
        from toil.server.utils import WorkflowStateMachine, WorkflowStateStore, MemoryStateStore
        
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

    def tearDown(self) -> None:
        super().tearDown()
        
    def _report_log(self, client: "FlaskClient", run_id: str) -> None:
        """
        Report the log for the given workflow run.
        """
        rv = client.get(f"/ga4gh/wes/v1/runs/{run_id}")
        self.assertEqual(rv.status_code, 200)
        self.assertTrue(rv.is_json)
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

    def _wait_for_success(self, client: "FlaskClient", run_id: str) -> None:
        """
        Wait for the given workflow run to succeed. If it fails, raise an exception.
        """

        while True:
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
            if state == "COMPLETE":
                # We're done!
                logger.info("Workflow reached state %s", state)
                return
            if state in ["EXECUTOR_ERROR", "SYSTEM_ERROR", "CANCELED"]:
                logger.error("Workflow run failed")
                self._report_log(client, run_id)
                raise RuntimeError("Workflow is in fail state " + state)
            logger.info("Waiting on workflow in state %s", state)
            time.sleep(2)
            

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
    
    def run_zip_workflow(self, zip_path: str, include_message: bool = True) -> None:
        """
        We have several zip file tests; this submits a zip file and makes sure it ran OK.
        """
        self.assertTrue(os.path.exists(zip_path))
        with self.app.test_client() as client:
            rv = client.post("/ga4gh/wes/v1/runs", data={
                "workflow_url": "file://" + zip_path,
                "workflow_type": "CWL",
                "workflow_type_version": "v1.0",
                "workflow_params": json.dumps({"message": "Hello, world!"} if include_message else {})
            })
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
            
    # TODO: When we can check the output value, add tests for overriding
    # packaged inputs.
        
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
    


if __name__ == "__main__":
    unittest.main()
