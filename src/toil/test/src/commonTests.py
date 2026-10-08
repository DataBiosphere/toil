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
from pathlib import Path

import pytest

from toil.common import Config, InconsistentConfigurationError, derive_run_dir_defaults

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
        # We need to only use modules we know will be installed, for the batch
        # system side.
        # Which means we can't use kubernetes.
        config = Config()
        config.batchSystem = "single_machine"
        config.provisioner = "aws"
        with pytest.raises(InconsistentConfigurationError) as info:
            config.check_configuration_consistency()
        assert "provisioner" in str(info.value)
        assert "single_machine" in str(info.value)
        assert "aws" in str(info.value)


class TestDeriveRunDirDefaults:
    """
    Tests for derive_run_dir_defaults, which backs --runDir.
    """

    def test_all_explicit_values_are_unchanged(self, tmp_path: Path) -> None:
        run_dir = tmp_path / "rundir"
        job_store = "file:/some/explicit/jobstore"
        work_dir = str(tmp_path / "explicit-work")
        coordination_dir = str(tmp_path / "explicit-coordination")

        result = derive_run_dir_defaults(
            str(run_dir), job_store, work_dir, coordination_dir
        )

        assert result == (str(run_dir), job_store, work_dir, coordination_dir)
        # run_dir is always created. Explicit paths are the caller's
        # responsibility; this function doesn't touch them.
        assert os.path.isdir(run_dir)
        assert not os.path.exists(work_dir)
        assert not os.path.exists(coordination_dir)

    def test_derives_all_three_under_run_dir(self, tmp_path: Path) -> None:
        run_dir = tmp_path / "rundir"

        result_run_dir, job_store, work_dir, coordination_dir = (
            derive_run_dir_defaults(str(run_dir), None, None, None)
        )

        assert result_run_dir == str(run_dir)
        assert job_store.startswith(f"file:{run_dir / 'jobstore-'}")
        assert work_dir == str(run_dir / "work")
        assert coordination_dir == str(run_dir / "coordination")

        # run_dir is created directly; work_dir, coordination_dir, and
        # the job store are only paths here and create themselves later.
        assert os.path.isdir(run_dir)
        assert not os.path.exists(work_dir)
        assert not os.path.exists(coordination_dir)
        assert not os.path.exists(job_store.removeprefix("file:"))

    def test_job_store_paths_are_unique_across_calls(self, tmp_path: Path) -> None:
        run_dir = tmp_path / "rundir"

        _, job_store_1, _, _ = derive_run_dir_defaults(str(run_dir), None, None, None)
        _, job_store_2, _, _ = derive_run_dir_defaults(str(run_dir), None, None, None)

        assert job_store_1 != job_store_2

    def test_mixed_explicit_and_derived(self, tmp_path: Path) -> None:
        run_dir = tmp_path / "rundir"
        explicit_work_dir = str(tmp_path / "explicit-work")

        _, job_store, work_dir, coordination_dir = derive_run_dir_defaults(
            str(run_dir), None, explicit_work_dir, None
        )

        assert work_dir == explicit_work_dir
        assert not os.path.exists(explicit_work_dir)
        assert job_store.startswith(f"file:{run_dir / 'jobstore-'}")
        assert coordination_dir == str(run_dir / "coordination")
        assert not os.path.exists(coordination_dir)

    def test_relative_run_dir_is_absolutized(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        result_run_dir, _, _, _ = derive_run_dir_defaults(
            "relative-rundir", None, None, None
        )
        assert result_run_dir == str(tmp_path / "relative-rundir")


