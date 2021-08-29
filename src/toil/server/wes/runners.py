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
from typing import List, Optional


class WorkflowRunner:
    """
    A class to represent a workflow runner of a particular type. Intended to be
    inherited. This class is responsible for generating a shell command that
    runs the requested workflow.
    """

    def __init__(self, run_id: str, version: str) -> None:
        self.run_id = run_id
        self.version = version

        self.cloud = False
        self.job_store: Optional[str] = None

    @classmethod
    def create(cls, run_id: str, version: str) -> "WorkflowRunner":
        return cls(run_id, version)

    @classmethod
    def supported_versions(cls) -> List[str]:
        """
        Get all the workflow versions that this runner implementation supports.
        """
        raise NotImplementedError

    def sort_options(self, job_store: str, out_dir: str, default_engine_options: List[str]) -> List[str]:
        """
        Sort the command line arguments in the order that can be recognized by
        the runner.
        """
        options = default_engine_options
        self.job_store = job_store

        for opt in options:
            if opt.startswith("--jobStore="):
                self.job_store = opt[11:]
                options.remove(opt)
            if opt.startswith(("--outdir=", "-o=")):
                options.remove(opt)
        if self.job_store.startswith(("aws", "google", "azure")):
            self.cloud = True

        self._sort_options(job_store, out_dir, options)
        return options

    def _sort_options(self, job_store: str, out_dir: str, options: List[str]) -> None:
        raise NotImplementedError

    def construct_command(self, workflow_url: str, input_json: str, options: List[str]) -> List[str]:
        """
        Return a list of shell commands that should be executed in order to
        complete this workflow run.
        """
        raise NotImplementedError


class PythonRunner(WorkflowRunner):
    """ Toil workflows."""

    @classmethod
    def supported_versions(cls) -> List[str]:
        return [
            "3.6", "3.7", "3.8", "3.9"
        ]

    def _sort_options(self, job_store: str, out_dir: str, options: List[str]) -> None:
        if not self.cloud:
            # TODO: find a way to communicate the out_dir to the Toil workflow
            pass
        # append the positional jobStore argument at the end for Toil workflows
        options.append(job_store)

    def construct_command(self, workflow_url: str, input_json: str, options: List[str]) -> List[str]:
        return (
                ["python"] + [workflow_url] + options
        )


class CWLRunner(WorkflowRunner):
    """ CWL workflows that are run with toil-cwl-runner."""

    @classmethod
    def supported_versions(cls) -> List[str]:
        return [
            "v1.0", "v1.1", "v1.2"
        ]

    def _sort_options(self, job_store: str, out_dir: str, options: List[str]) -> None:
        if not self.cloud:
            options.append("--outdir=" + out_dir)
        options.append("--jobStore=" + job_store)

    def construct_command(self, workflow_url: str, input_json: str, options: List[str]) -> List[str]:
        return (
                ["toil-cwl-runner"] + options + [workflow_url, input_json]
        )


class WDLRunner(WorkflowRunner):
    """ WDL workflows that are run with toil-wdl-runner."""

    @classmethod
    def supported_versions(cls) -> List[str]:
        return [
            "draft-2", "1.0"
        ]

    def _sort_options(self, job_store: str, out_dir: str, options: List[str]) -> None:
        if not self.cloud:
            options.append("--outdir=" + out_dir)
        options.append("--jobStore=" + job_store)

    def construct_command(self, workflow_url: str, input_json: str, options: List[str]) -> List[str]:
        return (
                ["toil-wdl-runner"] + options + [workflow_url, input_json]
        )
