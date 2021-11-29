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
import shutil
import subprocess
import textwrap
import unittest
import uuid
from io import BytesIO
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from flask import Flask

from toil.test import ToilTest, needs_server

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

        self.app: "Flask" = create_app(args).app
        self.app.testing = True

    def tearDown(self) -> None:
        super(ToilWESServerTest, self).tearDown()

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
        self.assertIn("default_workflow_engine_parameters", service_info)
        self.assertIn("system_state_counts", service_info)
        self.assertIn("tags", service_info)

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
            example_cwl = textwrap.dedent("""
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
            with self.app.test_client() as client:
                rv = client.post("/ga4gh/wes/v1/runs", data={
                    "workflow_url": "example.cwl",
                    "workflow_type": "CWL",
                    "workflow_type_version": "v1.0",
                    "workflow_params": json.dumps({"message": "Hello, world!"}),
                    "workflow_attachment": [
                        (BytesIO(example_cwl.encode()), "example.cwl"),
                    ],
                })
                # workflow is submitted successfully
                self.assertEqual(rv.status_code, 200)
                self.assertTrue(rv.is_json)
                run_id = rv.json.get("run_id")
                self.assertIsNotNone(run_id)

                # check outputs

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


if __name__ == "__main__":
    unittest.main()
