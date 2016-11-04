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
                                      '--never-download',  # prevent silent upgrades to pip etc
                                      'venv')
                leader.runOnAppliance('venv/bin/pip', 'list')  # For diagnostic purposes
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
                    assert False

                if __name__ == '__main__':
                    options = Job.Runner.getDefaultArgumentParser().parse_args()
                    with Toil(options) as toil:
                        if toil.config.restart:
                            toil.restart()
                        else:
                            toil.start(Job.wrapJobFn(job))

            userScript = self._getScriptSource(userScript)

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

            # Deploy an updated version of the script ...
            userScript = userScript.replace('assert False', 'assert True')
            leader.deployScript(path=self.sitePackages,
                                packagePath='foo.bar',
                                script=userScript)
            # ... and restart Toil.
            command = concat(pythonArgs, '--restart', toilArgs)
            leader.runOnAppliance(*command)

    def testSplitRootPackages(self):
        """
        Test whether hot-deployment works with a virtualenv in which jobs are defined in
        completely separate branches of the package hierarchy. Initially, hot deployment did
        deploy the entire virtualenv but jobs could only be defined in one branch of the package
        hierarchy. We define a branch as the maximum set of fully qualified package paths that
        share the same first component. IOW, a.b and a.c are in the same branch, while a.b and
        d.c are not.
        """
        with self._venvApplianceCluster() as (leader, worker):

            # Deploy the library module with job definitions
            def libraryModule():
                # noinspection PyUnusedLocal
                def libraryJob(job):
                    open('/data/foo.txt', 'w').close()

            leader.deployScript(path=self.sitePackages,
                                packagePath='toil_lib.foo',
                                script=libraryModule)

            # Deploy the user script
            def userScript():
                from toil.job import Job
                from toil.common import Toil
                # noinspection PyUnresolvedReferences
                from toil_lib.foo import libraryJob

                # noinspection PyUnusedLocal
                def job(job, disk='10M', cores=1, memory='10M'):
                    # Double the requirements to prevent chaining as chaining might hide problems
                    # in hot deployment code.
                    job.addChildJobFn(libraryJob, disk='20M', cores=cores, memory=memory)

                if __name__ == '__main__':
                    options = Job.Runner.getDefaultArgumentParser().parse_args()
                    with Toil(options) as toil:
                        if toil.config.restart:
                            toil.restart()
                        else:
                            toil.start(Job.wrapJobFn(job))

            leader.deployScript(path=self.sitePackages,
                                packagePath='toil_script.bar',
                                script=userScript)

            # Assert that output file isn't there
            worker.runOnAppliance('test', '!', '-f', '/data/foo.txt')
            # Just being paranoid
            self.assertRaises(CalledProcessError,
                              worker.runOnAppliance, 'test', '-f', '/data/foo.txt')
            leader.runOnAppliance('venv/bin/python',
                                  '-m', 'toil_script.bar',
                                  '--logDebug',
                                  '--batchSystem=mesos',
                                  '--mesosMaster=localhost:5050',
                                  '--defaultMemory=10M',
                                  '/data/jobstore')
            # Assert that out output file is there
            worker.runOnAppliance('test', '-f', '/data/foo.txt')

    def testUserTypesInJobFunctionArgs(self):
        """
        Test encapsulated, function-wrapping jobs where the function arguments reference
        user-defined types.

        Mainly written to cover https://github.com/BD2KGenomics/toil/issues/1259 but then also
        revealed https://github.com/BD2KGenomics/toil/issues/1278.
        """
        with self._venvApplianceCluster() as (leader, worker):
            def userScript():
                from toil.job import Job
                from toil.common import Toil

                # A user-defined type, i.e. a type defined in the user script
                class X(object):
                    pass

                # noinspection PyUnusedLocal
                def job(job, x, disk='10M', cores=1, memory='10M'):
                    return x

                if __name__ == '__main__':
                    options = Job.Runner.getDefaultArgumentParser().parse_args()
                    x = X()
                    with Toil(options) as toil:
                        r = toil.start(Job.wrapJobFn(job, x).encapsulate())
                    # Assert that the return value is of type X, but not X from the __main__
                    # module but X from foo.bar, the canonical name for the user module. The
                    # translation from __main__ to foo.bar is a side effect of hot-deployment.
                    assert r.__class__ is not X
                    import foo.bar
                    assert r.__class__ is foo.bar.X
                    # Assert that a copy was made. This is a side effect of pickling/unpickling.
                    assert x is not r

            userScript = self._getScriptSource(userScript)

            leader.deployScript(path=self.sitePackages,
                                packagePath='foo.bar',
                                script=userScript)

            leader.runOnAppliance('venv/bin/python', '-m', 'foo.bar',
                                  '--logDebug',
                                  '--batchSystem=mesos',
                                  '--mesosMaster=localhost:5050',
                                  '--defaultMemory=10M',
                                  '/data/jobstore')
