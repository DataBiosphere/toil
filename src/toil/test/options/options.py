from configargparse import ArgParser

from toil.common import Toil, addOptions
from toil.test import ToilTest
from toil.worker import main as worker_main
from toil.jobStores.utils import TOIL_WORKER_NO_JOB_STORE_EXIT_CODE


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
