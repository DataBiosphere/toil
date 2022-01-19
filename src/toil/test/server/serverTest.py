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
from typing import TYPE_CHECKING

from flask import Flask
from flask.testing import FlaskClient

from toil.test import ToilTest, needs_server, needs_celery_broker

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


@needs_server
class ToilServerUtilsTest(ToilTest):
    """
    Tests for the utility functions used by the Toil server.
    """


@needs_server
class ToilWESServerTest(ToilTest):
    """
    Tests for Toil's Workflow Execution Service API support using Flask's
    builtin test client.
    """

    def setUp(self) -> None:
        super(ToilWESServerTest, self).setUp()
        self.temp_dir = self._createTempDir()

        from toil.server.app import create_app, parser_with_server_options
        parser = parser_with_server_options()
        args = parser.parse_args(["--work_dir", os.path.join(self.temp_dir, "workflows")])

        self.app: Flask = create_app(args).app
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
        super(ToilWESServerTest, self).tearDown()

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

    def _report_log(self, client: FlaskClient, run_id: str) -> None:
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
        
    def _report_absolute_url(self, client: FlaskClient, url: str):
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

    def _wait_for_success(self, client: FlaskClient, run_id: str) -> None:
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

    @needs_celery_broker
    def test_run_example_cwl_workflow(self) -> None:
        """
        Test submitting the example CWL workflow to the WES server and getting
        the run status.
        """

        with self.subTest('Test run example CWL workflow from relative workflow URL but with no attachments.'):
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

        with self.subTest('Test run example CWL workflow from relative workflow URL.'):
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

        with self.subTest('Test run example CWL workflow from ZIP.'):
            workdir = self._createTempDir()
            zip_path = os.path.abspath(os.path.join(workdir, 'workflow.zip'))
            with zipfile.ZipFile(zip_path, 'w') as zip_file:
                zip_file.writestr('example.cwl', self.example_cwl)
            self.assertTrue(os.path.exists(zip_path))
            with self.app.test_client() as client:
                rv = client.post("/ga4gh/wes/v1/runs", data={
                    "workflow_url": "file://" + zip_path,
                    "workflow_type": "CWL",
                    "workflow_type_version": "v1.0",
                    "workflow_params": json.dumps({"message": "Hello, world!"})
                })
                # workflow is submitted successfully
                self.assertEqual(rv.status_code, 200)
                self.assertTrue(rv.is_json)
                run_id = rv.json.get("run_id")
                self.assertIsNotNone(run_id)

                # Check status
                self._wait_for_success(client, run_id)

        with self.subTest('Test run example CWL workflow from the Internet.'):
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


if __name__ == "__main__":
    unittest.main()
