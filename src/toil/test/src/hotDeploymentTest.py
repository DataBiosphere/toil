import logging
from inspect import getsource
from subprocess import CalledProcessError
from textwrap import dedent

from bd2k.util.iterables import concat

from toil.test import needs_mesos, ApplianceTestSupport

log = logging.getLogger(__name__)


@needs_mesos
class HotDeploymentTest(ApplianceTestSupport):
    def setUp(self):
        logging.basicConfig(level=logging.INFO)
        super(HotDeploymentTest, self).setUp()

    def test(self):
        dataDirPath = self._createTempDir(purpose='data')
        with self._applianceCluster(mounts={dataDirPath: '/data'}) as (leader, worker):
            leader.runOnAppliance('virtualenv',
                                  '--system-site-packages',
                                  '--never-download',
                                  'venv')
            leader.runOnAppliance('venv/bin/pip', 'list')

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

            package = 'venv/lib/python2.7/site-packages/foo'
            leader.runOnAppliance('mkdir', '-p', package)
            leader.writeToAppliance(package + '/__init__.py', '')
            script = dedent('\n'.join(getsource(userScript).split('\n')[1:]))
            leader.writeToAppliance(package + '/bar.py', script)
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
