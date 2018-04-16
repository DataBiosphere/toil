# Copyright (C) 2015 Curoverse, Inc
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
from __future__ import absolute_import
from __future__ import print_function
import json
import os
from toil import subprocess
import unittest
import re
import shutil
import pytest
from future.moves.urllib.request import urlretrieve
import zipfile

# Python 3 compatibility imports
from six.moves import StringIO
from six import u as str

from toil.test import (ToilTest, needs_cwl, slow, needs_docker, needs_lsf,
                       needs_mesos, needs_parasol, needs_gridengine, needs_slurm,
                       needs_torque)


@needs_cwl
class CWLTest(ToilTest):

    def _tester(self, cwlfile, jobfile, outDir, expect, main_args=[], out_name="output"):
        from toil.cwl import cwltoil
        rootDir = self._projectRootPath()
        st = StringIO()
        main_args = main_args[:]
        main_args.extend(['--outdir', outDir, os.path.join(rootDir, cwlfile), os.path.join(rootDir, jobfile)])
        cwltoil.main(main_args, stdout=st)
        out = json.loads(st.getvalue())
        out[out_name].pop("http://commonwl.org/cwltool#generation", None)
        out[out_name].pop("nameext", None)
        out[out_name].pop("nameroot", None)
        self.assertEquals(out, expect)

    def _debug_worker_tester(self, cwlfile, jobfile, outDir, expect):
        from toil.cwl import cwltoil
        rootDir = self._projectRootPath()
        st = StringIO()
        cwltoil.main(['--debugWorker', '--outdir', outDir,
                     os.path.join(rootDir, cwlfile),
                     os.path.join(rootDir, jobfile)], stdout=st)
        out = json.loads(st.getvalue())
        out["output"].pop("http://commonwl.org/cwltool#generation", None)
        out["output"].pop("nameext", None)
        out["output"].pop("nameroot", None)
        self.assertEquals(out, expect)

    def test_run_revsort(self):
        outDir = self._createTempDir()
        self._tester('src/toil/test/cwl/revsort.cwl',
                     'src/toil/test/cwl/revsort-job.json',
                     outDir, {
            # Having unicode string literals isn't necessary for the assertion but makes for a
            # less noisy diff in case the assertion fails.
            u'output': {
                u'location': "file://" + str(os.path.join(outDir, 'output.txt')),
                u'basename': str("output.txt"),
                u'size': 1111,
                u'class': u'File',
                u'checksum': u'sha1$b9214658cc453331b62c2282b772a5c063dbd284'}})

    def test_run_revsort_debug_worker(self):
        outDir = self._createTempDir()
        # Having unicode string literals isn't necessary for the assertion
        # but makes for a less noisy diff in case the assertion fails.
        self._debug_worker_tester(
                'src/toil/test/cwl/revsort.cwl',
                'src/toil/test/cwl/revsort-job.json', outDir, {u'output': {
                    u'location': "file://" + str(os.path.join(
                        outDir, 'output.txt')),
                    u'basename': str("output.txt"),
                    u'size': 1111,
                    u'class': u'File',
                    u'checksum':
                        u'sha1$b9214658cc453331b62c2282b772a5c063dbd284'}})

    @slow
    def test_restart(self):
        """Enable restarts with CWLtoil -- run failing test, re-run correct test.
        """
        from toil.cwl import cwltoil
        from toil.jobStores.abstractJobStore import NoSuchJobStoreException
        from toil.leader import FailedJobsException
        outDir = self._createTempDir()
        cwlDir = os.path.join(self._projectRootPath(), "src", "toil", "test", "cwl")
        cmd = ['--outdir', outDir, '--jobStore', os.path.join(outDir, 'jobStore'), "--no-container",
               os.path.join(cwlDir, "revsort.cwl"), os.path.join(cwlDir, "revsort-job.json")]

        def path_without_rev():
            return ":".join([d for d in os.environ["PATH"].split(":")
                             if not os.path.exists(os.path.join(d, "rev"))])
        orig_path = os.environ["PATH"]
        # Force a failure and half finished job by removing `rev` from the PATH
        os.environ["PATH"] = path_without_rev()
        try:
            cwltoil.main(cmd)
            self.fail("Expected problem job with incorrect PATH did not fail")
        except FailedJobsException:
            pass
        # Finish the job with a correct PATH
        os.environ["PATH"] = orig_path
        cwltoil.main(["--restart"] + cmd)
        # Should fail because previous job completed successfully
        try:
            cwltoil.main(["--restart"] + cmd)
            self.fail("Restart with missing directory did not fail")
        except NoSuchJobStoreException:
            pass

    @slow
    @pytest.mark.timeout(1800)
    def test_run_conformance(self, batchSystem=None):
        rootDir = self._projectRootPath()
        cwlSpec = os.path.join(rootDir, 'src/toil/test/cwl/spec')
        # The latest cwl git hash. Update it to get the latest tests.
        testhash = "f96bca6911b6688ff614c02dbefe819bed260a13"
        url = "https://github.com/common-workflow-language/common-workflow-language/archive/%s.zip" % testhash
        if not os.path.exists(cwlSpec):
            urlretrieve(url, "spec.zip")
            with zipfile.ZipFile('spec.zip', "r") as z:
                z.extractall()
            shutil.move("common-workflow-language-%s" % testhash, cwlSpec)
            os.remove("spec.zip")
        try:
            cmd = ["bash", "run_test.sh", "RUNNER=toil-cwl-runner",
                   "DRAFT=v1.0", "-j4"]
            if batchSystem:
                cmd.extend(["--batchSystem", batchSystem])
            subprocess.check_output(cmd, cwd=cwlSpec, stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as e:
            only_unsupported = False
            # check output -- if we failed but only have unsupported features, we're okay
            p = re.compile(r"(?P<failures>\d+) failures, (?P<unsupported>\d+) unsupported features")
            for line in e.output.split("\n"):
                m = p.search(line)
                if m:
                    if int(m.group("failures")) == 0 and int(m.group("unsupported")) > 0:
                        only_unsupported = True
                        break
            if not only_unsupported:
                print(e.output)
                raise e

    @slow
    def test_bioconda(self):
        outDir = self._createTempDir()
        self._tester('src/toil/test/cwl/seqtk_seq.cwl',
                     'src/toil/test/cwl/seqtk_seq_job.json',
                     outDir,
                     self._expected_seqtk_output(outDir),
                     main_args=["--beta-conda-dependencies"],
                     out_name="output1")

    @needs_docker
    def test_biocontainers(self):
        outDir = self._createTempDir()
        self._tester('src/toil/test/cwl/seqtk_seq.cwl',
                     'src/toil/test/cwl/seqtk_seq_job.json',
                     outDir,
                     self._expected_seqtk_output(outDir),
                     main_args=["--beta-use-biocontainers"],
                     out_name="output1")

    @slow
    @needs_lsf
    @unittest.skip
    def test_lsf_cwl_conformance(self):
        return self.test_run_conformance("LSF")

    @slow
    @needs_slurm
    @unittest.skip
    def test_slurm_cwl_conformance(self):
        return self.test_run_conformance("Slurm")

    @slow
    @needs_torque
    @unittest.skip
    def test_torque_cwl_conformance(self):
        return self.test_run_conformance("Torque")

    @slow
    @needs_gridengine
    @unittest.skip
    def test_gridengine_cwl_conformance(self):
        return self.test_run_conformance("gridEngine")

    @slow
    @needs_mesos
    @unittest.skip
    def test_mesos_cwl_conformance(self):
        return self.test_run_conformance("mesos")

    @slow
    @needs_parasol
    @unittest.skip
    def test_parasol_cwl_conformance(self):
        return self.test_run_conformance("parasol")

    def _expected_seqtk_output(self, outDir):
        return {
            u"output1":  {
                u"location": "file://" + str(os.path.join(outDir, 'out')),
                u"checksum": u"sha1$322e001e5a99f19abdce9f02ad0f02a17b5066c2",
                u"basename": str("out"),
                u"class": u"File",
                u"size": 150}
        }
