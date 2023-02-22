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


import os
import stat
import uuid

from toil.common import Toil
from toil.fileStores import FileID
from toil.job import Job
from toil.exceptions import FailedJobsException
from toil.test import ToilTest, slow


class ImportExportFileTest(ToilTest):
    def setUp(self):
        super().setUp()
        self.tmp_dir = self._createTempDir()
        self.output_file_path = f'{self.tmp_dir}/out'
        self.message_portion_1 = 'What do you get when you cross a seal and a polar bear?'
        self.message_portion_2 = '  A polar bear.'

    def create_file(self, content, executable=False):
        file_path = f'{self.tmp_dir}/{uuid.uuid4()}'

        with open(file_path, 'w') as f:
            f.write(content)

        if executable:
            # Add file owner execute permissions
            os.chmod(file_path, os.stat(file_path).st_mode | stat.S_IXUSR)

        return file_path

    def _import_export_workflow(self, options, fail):
        with Toil(options) as toil:
            if not options.restart:
                msg_portion_file_path = self.create_file(content=self.message_portion_1)
                msg_portion_file_id = toil.importFile(f'file://{msg_portion_file_path}')
                self.assertIsInstance(msg_portion_file_id, FileID)
                self.assertEqual(os.stat(msg_portion_file_path).st_size, msg_portion_file_id.size)

                file_that_can_trigger_failure_when_job_starts = self.create_file(
                    content='Time to freak out!' if fail else 'Keep calm and carry on.')
                self.trigger_file_id = toil.importFile(f'file://{file_that_can_trigger_failure_when_job_starts}')
                workflow_final_output_file_id = toil.start(
                    RestartingJob(msg_portion_file_id, self.trigger_file_id, self.message_portion_2))
            else:
                # TODO: We're hackily updating this file without using the
                #  correct FileStore interface. User code should not do this!
                with toil._jobStore.update_file_stream(self.trigger_file_id) as f:
                    f.write(('Time to freak out!' if fail else 'Keep calm and carry on.').encode('utf-8'))

                workflow_final_output_file_id = toil.restart()

            toil.exportFile(workflow_final_output_file_id, f'file://{self.output_file_path}')
            with open(self.output_file_path) as f:
                self.assertEqual(f.read(), f'{self.message_portion_1}{self.message_portion_2}')

    def _run_import_export_workflow(self, restart):
        options = Job.Runner.getDefaultOptions(self._getTestJobStorePath())
        options.logLevel = "INFO"

        if restart:
            try:
                self._import_export_workflow(options, fail=True)
            except FailedJobsException:
                options.restart = True

        self._import_export_workflow(options, fail=False)

    @slow
    def test_import_export_restart_true(self):
        self._run_import_export_workflow(restart=True)

    def test_import_export_restart_false(self):
        self._run_import_export_workflow(restart=False)

    def test_basic_import_export(self):
        """
        Ensures that uploaded files preserve their file permissions when they
        are downloaded again. This function checks that an imported executable file
        maintains its executability after being exported.
        """
        options = Job.Runner.getDefaultOptions(self._getTestJobStorePath())
        options.logLevel = "INFO"

        with Toil(options) as toil:
            # TODO: test this with non-local (AWS, Google)
            #  Note: this is somewhat done in src/toil/test/src/fileStoreTest.py
            with self.subTest('Testing permissions are preserved for local importFile/exportFile'):
                for executable in True, False:
                    file_path = self.create_file(content='Hello', executable=executable)
                    initial_permissions = os.stat(file_path).st_mode & stat.S_IXUSR
                    file_id = toil.importFile(f'file://{file_path}')
                    toil.exportFile(file_id, f'file://{self.output_file_path}')
                    current_permissions = os.stat(self.output_file_path).st_mode & stat.S_IXUSR
                    assert initial_permissions == current_permissions

            with self.subTest('Testing relative paths without the file:// schema.'):
                relative_path_data = 'Everything is relative.'
                file_path = self.create_file(content=relative_path_data)

                file_id = toil.importFile(os.path.relpath(file_path))
                toil.exportFile(file_id, os.path.relpath(self.output_file_path))
                with open(self.output_file_path) as f:
                    self.assertEqual(f.read(), relative_path_data)

            with self.subTest('Test local importFile accepts a shared_file_name.'):
                # TODO: whyyyy do we allow this?  shared file names are not unique and can overwrite each other
                #  ...not only that... we can't use exportFile on them afterwards!?
                file_path = self.create_file(content='why')
                shared_file_name = 'users_should_probably_not_be_allowed_to_make_shared_files.bad'
                toil.importFile(f'file://{file_path}', sharedFileName=shared_file_name)
                with toil._jobStore.read_shared_file_stream(shared_file_name, encoding='utf-8') as f:
                    self.assertEqual(f.read(), 'why')


class RestartingJob(Job):
    def __init__(self, msg_portion_file_id, trigger_file_id, message_portion_2):
        Job.__init__(self,  memory=100000, cores=1, disk="1M")
        self.msg_portion_file_id = msg_portion_file_id
        self.trigger_file_id = trigger_file_id
        self.message_portion_2 = message_portion_2

    def run(self, file_store):
        with file_store.readGlobalFileStream(self.trigger_file_id) as readable:
            if readable.read() == b'Time to freak out!':
                raise RuntimeError('D:')

        with file_store.writeGlobalFileStream() as (writable, output_file_id):
            with file_store.readGlobalFileStream(self.msg_portion_file_id, encoding='utf-8') as readable:
                # combine readable.read() (the original message 1) with message 2
                # this will be the final output of the workflow
                writable.write(f'{readable.read()}{self.message_portion_2}'.encode())
                return output_file_id
