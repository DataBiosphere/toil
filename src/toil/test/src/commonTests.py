# Copyright (C) 2015-2026 Regents of the University of California
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
import os
import sys

import pytest

from toil.common import Config, InconsistentConfigurationError

logger = logging.getLogger(__name__)
logging.basicConfig()

class TestConfig:
    """
    Tests for the Toil configuration object.
    """

    def test_check_configuration_consistency_disallows_bad_scaling_setup(self) -> None:
        """
        Make sure we're not allowed to try and do autoscaling in the workflow with a batch system that can't handle it.
        """
        config = Config()
        config.batchSystem = "kubernetes"
        config.provisioner = "aws"
        with pytest.raises(InconsistentConfigurationError) as info:
            config.check_configuration_consistency()
        assert "provisioner" in str(info.value)
        assert "kubernetes" in str(info.value)
        assert "aws" in str(info.value)



