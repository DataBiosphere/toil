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


from argparse import Namespace
import os
from pathlib import Path
import stat
import uuid

from toil.common import Toil
from toil.exceptions import FailedJobsException
from toil.fileStores import FileID
from toil.fileStores.abstractFileStore import AbstractFileStore
from toil.job import Job
from toil.test import pslow as slow

import pytest
from pytest_subtests import SubTests


def create_file(tmp_path: Path, content: str, executable: bool = False) -> Path:
    file_path = tmp_path / str(uuid.uuid4())

    with file_path.open("w") as f:
        f.write(content)

    if executable:
        # Add file owner execute permissions
        file_path.chmod(file_path.stat().st_mode | stat.S_IXUSR)

    return file_path


class TestImportExportFile:
    message_portion_1 = "What do you get when you cross a seal and a polar bear?"
    message_portion_2 = "  A polar bear."

    def _import_export_workflow(
        self, tmp_path: Path, options: Namespace, fail: bool
    ) -> None:
        with Toil(options) as toil:
            if not options.restart:
                msg_portion_file_path = create_file(
                    tmp_path, content=self.message_portion_1
                )
                msg_portion_file_id = toil.importFile(msg_portion_file_path.as_uri())
                assert isinstance(msg_portion_file_id, FileID)
                assert msg_portion_file_path.stat().st_size == msg_portion_file_id.size

                file_that_can_trigger_failure_when_job_starts = create_file(
                    tmp_path,
                    content="Time to freak out!" if fail else "Keep calm and carry on.",
                )
                self.trigger_file_id = toil.importFile(
                    file_that_can_trigger_failure_when_job_starts.as_uri()
                )
                workflow_final_output_file_id = toil.start(
                    RestartingJob(
                        msg_portion_file_id,
                        self.trigger_file_id,
                        self.message_portion_2,
                    )
                )
            else:
                # TODO: We're hackily updating this file without using the
                #  correct FileStore interface. User code should not do this!
                with toil._jobStore.update_file_stream(self.trigger_file_id) as f:
                    f.write(
                        (
                            "Time to freak out!" if fail else "Keep calm and carry on."
                        ).encode("utf-8")
                    )

                workflow_final_output_file_id = toil.restart()

            toil.exportFile(workflow_final_output_file_id, str(tmp_path / "out"))
            with (tmp_path / "out").open() as f:
                assert f.read() == f"{self.message_portion_1}{self.message_portion_2}"

    def _run_import_export_workflow(self, tmp_path: Path, restart: bool) -> None:
        options = Job.Runner.getDefaultOptions(tmp_path / "jobstore")
        options.logLevel = "INFO"

        if restart:
            try:
                self._import_export_workflow(tmp_path, options, fail=True)
            except FailedJobsException:
                options.restart = True

        self._import_export_workflow(tmp_path, options, fail=False)

    @slow
    @pytest.mark.slow
    def test_import_export_restart_true(self, tmp_path: Path) -> None:
        self._run_import_export_workflow(tmp_path, restart=True)

    def test_import_export_restart_false(self, tmp_path: Path) -> None:
        self._run_import_export_workflow(tmp_path, restart=False)

    def test_basic_import_export(self, tmp_path: Path, subtests: SubTests) -> None:
        """
        Ensures that uploaded files preserve their file permissions when they
        are downloaded again. This function checks that an imported executable file
        maintains its executability after being exported.
        """
        options = Job.Runner.getDefaultOptions(tmp_path / "jobstore")
        options.logLevel = "INFO"

        with Toil(options) as toil:
            # TODO: test this with non-local (AWS, Google)
            #  Note: this is somewhat done in src/toil/test/src/fileStoreTest.py

            with subtests.test(
                msg="Testing permissions are preserved for local importFile/exportFile"
            ):
                for executable in True, False:
                    file_path = create_file(
                        tmp_path, content="Hello", executable=executable
                    )
                    initial_permissions = file_path.stat().st_mode & stat.S_IXUSR
                    file_id = toil.importFile(file_path.as_uri())
                    output_file_path = tmp_path / f"out_{executable}"
                    toil.exportFile(file_id, output_file_path.as_uri())
                    current_permissions = output_file_path.stat().st_mode & stat.S_IXUSR
                    assert initial_permissions == current_permissions

            with subtests.test(
                msg="Testing relative paths without the file:// schema."
            ):
                relative_path_data = "Everything is relative."
                file_path = create_file(tmp_path, content=relative_path_data)

                file_id = toil.importFile(os.path.relpath(file_path))
                output_file_path = tmp_path / "out2"
                toil.exportFile(file_id, os.path.relpath(output_file_path))
                with output_file_path.open() as f:
                    assert f.read() == relative_path_data

            with subtests.test(msg="Test local importFile accepts a shared_file_name."):
                # TODO: whyyyy do we allow this?  shared file names are not unique and can overwrite each other
                #  ...not only that... we can't use exportFile on them afterwards!?
                file_path = create_file(tmp_path, content="why")
                shared_file_name = (
                    "users_should_probably_not_be_allowed_to_make_shared_files.bad"
                )
                toil.importFile(file_path.as_uri(), sharedFileName=shared_file_name)
                with toil._jobStore.read_shared_file_stream(
                    shared_file_name, encoding="utf-8"
                ) as f:
                    assert f.read() == "why"


class RestartingJob(Job):
    def __init__(
        self, msg_portion_file_id: str, trigger_file_id: str, message_portion_2: str
    ) -> None:
        Job.__init__(self, memory=100000, cores=1, disk="1M")
        self.msg_portion_file_id = msg_portion_file_id
        self.trigger_file_id = trigger_file_id
        self.message_portion_2 = message_portion_2

    def run(self, file_store: AbstractFileStore) -> str:
        with file_store.readGlobalFileStream(self.trigger_file_id) as readable:
            if readable.read() == b"Time to freak out!":
                raise RuntimeError("D:")

        with file_store.writeGlobalFileStream() as (writable, output_file_id):
            with file_store.readGlobalFileStream(
                self.msg_portion_file_id, encoding="utf-8"
            ) as readable:
                # combine readable.read() (the original message 1) with message 2
                # this will be the final output of the workflow
                writable.write(f"{readable.read()}{self.message_portion_2}".encode())
                return output_file_id
