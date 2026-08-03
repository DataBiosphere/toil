import os

from configargparse import ArgParser

from toil.common import Toil, addOptions
from toil.test import ToilTest
from toil.worker import main as worker_main
from toil.jobStores.abstractJobStore import TOIL_WORKER_NO_JOB_STORE_EXIT_CODE


class OptionsTest(ToilTest):
    """
    Class to test functionality of all Toil options
    """

    def test_default_caching_slurm(self):
        """
        Test to ensure that caching will be set to false when running on Slurm
        :return:
        """
        parser = ArgParser()
        addOptions(parser, jobstore_as_flag=True, wdl=False, cwl=False)
        test_args = ["--jobstore=example-jobstore", "--batchSystem=slurm"]
        options = parser.parse_args(test_args)
        with Toil(options) as toil:
            caching_value = toil.config.caching
        self.assertEqual(caching_value, False)

    def test_worker_exits_with_sentinel_code_on_missing_job_store(self):
        """Worker should exit with TOIL_WORKER_NO_JOB_STORE_EXIT_CODE when the job store is unreachable."""
        with self.assertRaises(SystemExit) as cm:
            worker_main(["_toil_worker", "test_job", "file:/nonexistent/path/jobstore", "some_job_id"])
        self.assertEqual(cm.exception.code, TOIL_WORKER_NO_JOB_STORE_EXIT_CODE)

    def test_caching_option_priority(self):
        """
        Test to ensure that the --caching option takes priority over the default_caching() return value
        :return:
        """
        parser = ArgParser()
        addOptions(parser, jobstore_as_flag=True, wdl=False, cwl=False)
        # the kubernetes batchsystem (and I think all batchsystems including singlemachine) return False
        # for default_caching
        test_args = [
            "--jobstore=example-jobstore",
            "--batchSystem=kubernetes",
            "--caching=True",
        ]
        options = parser.parse_args(test_args)
        with Toil(options) as toil:
            caching_value = toil.config.caching
        self.assertEqual(caching_value, True)

    def test_workdir_created_if_missing(self):
        """
        --workDir should be created automatically if it doesn't exist.
        """
        parser = ArgParser()
        addOptions(parser, jobstore_as_flag=True, wdl=False, cwl=False)
        work_dir = os.path.join(self._createTempDir(), "missing-workdir")
        test_args = [
            f"--jobstore=file:{self._getTestJobStorePath()}",
            f"--workDir={work_dir}",
        ]
        options = parser.parse_args(test_args)
        self.assertFalse(os.path.exists(work_dir))
        with Toil(options):
            pass
        self.assertTrue(os.path.isdir(work_dir))

    def test_coordination_dir_created_if_missing(self):
        """
        --coordinationDir should be created automatically if it doesn't exist.
        """
        parser = ArgParser()
        addOptions(parser, jobstore_as_flag=True, wdl=False, cwl=False)
        coordination_dir = os.path.join(self._createTempDir(), "missing-coordination")
        test_args = [
            f"--jobstore=file:{self._getTestJobStorePath()}",
            f"--coordinationDir={coordination_dir}",
        ]
        options = parser.parse_args(test_args)
        self.assertFalse(os.path.exists(coordination_dir))
        with Toil(options):
            pass
        self.assertTrue(os.path.isdir(coordination_dir))

    def test_rundir_derives_workdir_and_coordination_dir(self):
        """
        --runDir should derive workDir/coordinationDir when they aren't explicitly set.
        """
        parser = ArgParser()
        addOptions(parser, jobstore_as_flag=True, wdl=False, cwl=False)
        run_dir = self._createTempDir()
        test_args = [
            f"--jobstore=file:{self._getTestJobStorePath()}",
            f"--runDir={run_dir}",
        ]
        options = parser.parse_args(test_args)
        with Toil(options) as toil:
            config = toil.config
        self.assertEqual(config.workDir, os.path.join(run_dir, "work"))
        self.assertEqual(
            config.coordination_dir, os.path.join(run_dir, "coordination")
        )

    def test_rundir_derives_jobstore_when_omitted(self):
        """
        --runDir should derive the job store location when --jobstore is not given.
        Only reachable for direct callers of the flag-based parser
        (jobstore_as_flag=True); the CWL/WDL runners fill in their own jobStore
        default before Config.setOptions ever runs, so they never hit this
        branch in practice.
        """
        parser = ArgParser()
        addOptions(parser, jobstore_as_flag=True, wdl=False, cwl=False)
        run_dir = self._createTempDir()
        options = parser.parse_args([f"--runDir={run_dir}"])
        with Toil(options) as toil:
            config = toil.config
        self.assertEqual(config.jobStore, f"file:{os.path.join(run_dir, 'jobstore')}")

    def test_explicit_workdir_overrides_rundir(self):
        """
        An explicit --workDir should win over the --runDir-derived default.
        """
        parser = ArgParser()
        addOptions(parser, jobstore_as_flag=True, wdl=False, cwl=False)
        run_dir = self._createTempDir()
        explicit_work_dir = self._createTempDir()
        test_args = [
            f"--jobstore=file:{self._getTestJobStorePath()}",
            f"--runDir={run_dir}",
            f"--workDir={explicit_work_dir}",
        ]
        options = parser.parse_args(test_args)
        with Toil(options) as toil:
            config = toil.config
        self.assertEqual(config.workDir, explicit_work_dir)
        self.assertFalse(os.path.exists(os.path.join(run_dir, "work")))

    def test_resolved_paths_are_logged(self):
        """
        Entering Toil(options) should log the resolved job store, work dir,
        and coordination dir at INFO.
        """
        parser = ArgParser()
        addOptions(parser, jobstore_as_flag=True, wdl=False, cwl=False)
        job_store = self._getTestJobStorePath()
        options = parser.parse_args([f"--jobstore=file:{job_store}"])
        with self.assertLogs("toil.common", level="INFO") as cm:
            with Toil(options):
                pass
        self.assertTrue(
            any("Resolved Toil run paths" in message for message in cm.output)
        )
