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
from typing import List


class WorkflowRunner:
    """
    A class to represent a workflow runner of a particular type. Intended to be
    inherited. This class is responsible for generating a shell command that
    runs the requested workflow.
    """

    def __init__(self) -> None:
        pass

    @classmethod
    def supported_versions(cls) -> List[str]:
        """
        Get all the workflow versions that this runner implementation supports.
        """
        raise NotImplementedError

    def construct_command(self) -> List[str]:
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

    def construct_command(self) -> List[str]:
        return []


class CWLRunner(WorkflowRunner):
    """ CWL workflows that are run with toil-cwl-runner."""

    @classmethod
    def supported_versions(cls) -> List[str]:
        return [
            "v1.0", "v1.1", "v1.2"
        ]

    def construct_command(self) -> List[str]:
        return []


class WDLRunner(WorkflowRunner):
    """ WDL workflows that are run with toil-wdl-runner."""

    @classmethod
    def supported_versions(cls) -> List[str]:
        return [
            "draft-2", "1.0"
        ]

    def construct_command(self) -> List[str]:
        return []
