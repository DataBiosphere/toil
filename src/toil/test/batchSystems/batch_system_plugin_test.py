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
import logging
from typing import Optional

from configargparse import ArgParser, ArgumentParser

from toil.batchSystems.abstractBatchSystem import (
    AbstractBatchSystem,
    UpdatedBatchJobInfo,
)
from toil.batchSystems.cleanup_support import BatchSystemCleanupSupport
from toil.batchSystems.options import OptionSetter
from toil.batchSystems.registry import add_batch_system_factory
from toil.common import Toil, addOptions
from toil.job import JobDescription
from toil.test import ToilTest

logger = logging.getLogger(__name__)


class FakeBatchSystem(BatchSystemCleanupSupport):
    @classmethod
    def supportsAutoDeployment(cls) -> bool:
        pass

    def issueBatchJob(
        self,
        command: str,
        job_desc: JobDescription,
        job_environment: Optional[dict[str, str]] = None,
    ) -> int:
        pass

    def killBatchJobs(self, jobIDs: list[int]) -> None:
        pass

    def getIssuedBatchJobIDs(self) -> list[int]:
        pass

    def getRunningBatchJobIDs(self) -> dict[int, float]:
        pass

    def getUpdatedBatchJob(self, maxWait: int) -> Optional[UpdatedBatchJobInfo]:
        pass

    def shutdown(self) -> None:
        pass

    @classmethod
    def add_options(cls, parser: ArgumentParser) -> None:
        parser.add_argument("--fake_argument", default="exists")

    @classmethod
    def setOptions(cls, setOption: OptionSetter) -> None:
        setOption("fake_argument")


class BatchSystemPluginTest(ToilTest):
    def test_batchsystem_plugin_installable(self):
        """
        Test that installing a batch system plugin works.
        :return:
        """

        def fake_batch_system_factory() -> type[AbstractBatchSystem]:
            return FakeBatchSystem

        add_batch_system_factory("fake", fake_batch_system_factory)

        parser = ArgParser()
        addOptions(parser)

        options = parser.parse_args(["test-jobstore", "--clean=always"])

        # try to install a batchsystem plugin with some arguments
        # if the arguments exists, the values should also exist in the config
        with Toil(options) as toil:
            self.assertEqual(toil.config.fake_argument == "exists", True)
