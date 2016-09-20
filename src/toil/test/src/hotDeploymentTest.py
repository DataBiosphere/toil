import logging
from contextlib import contextmanager
from subprocess import CalledProcessError

from bd2k.util.iterables import concat

from toil.test import needs_mesos, ApplianceTestSupport

log = logging.getLogger(__name__)


@needs_mesos
class HotDeploymentTest(ApplianceTestSupport):
    def setUp(self):
        logging.basicConfig(level=logging.INFO)
        super(HotDeploymentTest, self).setUp()

    @contextmanager
    def _venvApplianceCluster(self):
        """
        Creates an appliance cluster with a virtualenv at './venv' on the leader and a temporary
        directory on the host mounted at /data in the leader and worker containers.
        """
        dataDirPath = self._createTempDir(purpose='data')
        with self._applianceCluster(mounts={dataDirPath: '/data'}) as (leader, worker):
            try:
                leader.runOnAppliance('virtualenv',
                                      '--system-site-packages',
                                      '--never-download',
                                      'venv')
                leader.runOnAppliance('venv/bin/pip', 'list') # For diagnostic purposes
                yield leader, worker
            finally:
                # Without this step, we would leak files owned by root on the host's file system
                leader.runOnAppliance('rm', '-rf', '/data/*')

    sitePackages = 'venv/lib/python2.7/site-packages'

    def testRestart(self):
        """
        Test whether hot-deployment works on restart.
        """
        with self._venvApplianceCluster() as (leader, worker):
            def userScript():
                from toil.job import Job
                from toil.common import Toil

                # noinspection PyUnusedLocal
                def job(job, disk='10M', cores=1, memory='10M'):
                    # make job fail during the first invocation
                    assert job.fileStore.jobStore.config.restart

                if __name__ == '__main__':
                    options = Job.Runner.getDefaultArgumentParser().parse_args()
                    with Toil(options) as toil:
                        if toil.config.restart:
                            toil.restart()
                        else:
                            toil.start(Job.wrapJobFn(job))

            leader.deployScript(path=self.sitePackages,
                                packagePath='foo.bar',
                                script=userScript)

            pythonArgs = ['venv/bin/python', '-m', 'foo.bar']
            toilArgs = ['--logDebug',
                        '--batchSystem=mesos',
                        '--mesosMaster=localhost:5050',
                        '--defaultMemory=10M',
                        '/data/jobstore']
            command = concat(pythonArgs, toilArgs)
            self.assertRaises(CalledProcessError, leader.runOnAppliance, *command)
            command = concat(pythonArgs, '--restart', toilArgs)
            leader.runOnAppliance(*command)
