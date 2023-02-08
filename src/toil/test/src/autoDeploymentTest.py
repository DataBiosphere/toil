import logging
import subprocess
import time
from contextlib import contextmanager

from toil.lib.iterables import concat
from toil.test import (ApplianceTestSupport,
                       needs_local_appliance,
                       needs_mesos,
                       slow)
from toil.version import exactPython
from toil.exceptions import FailedJobsException

logger = logging.getLogger(__name__)


@needs_mesos
@needs_local_appliance
@slow
class AutoDeploymentTest(ApplianceTestSupport):
    """
    Tests various auto-deployment scenarios. Using the appliance, i.e. a docker container,
    for these tests allows for running worker processes on the same node as the leader process
    while keeping their file systems separate from each other and the leader process. Separate
    file systems are crucial to prove that auto-deployment does its job.
    """

    def setUp(self):
        logging.basicConfig(level=logging.INFO)
        super().setUp()

    @contextmanager
    def _venvApplianceCluster(self):
        """
        Creates an appliance cluster with a virtualenv at './venv' on the leader and a temporary
        directory on the host mounted at /data in the leader and worker containers.
        """
        dataDirPath = self._createTempDir(purpose='data')
        with self._applianceCluster(mounts={dataDirPath: '/data'}) as (leader, worker):
            leader.runOnAppliance('virtualenv',
                                  '--system-site-packages',
                                  '--never-download',  # prevent silent upgrades to pip etc
                                  '--python', exactPython,
                                  'venv')
            leader.runOnAppliance('venv/bin/pip', 'list')  # For diagnostic purposes
            yield leader, worker

    # TODO: Are we sure the python in the appliance we are testing is the same
    # as the one we are testing from? If not, how can we get the version it is?
    sitePackages = f'venv/lib/{exactPython}/site-packages'

    def testRestart(self):
        """
        Test whether auto-deployment works on restart.
        """
        with self._venvApplianceCluster() as (leader, worker):
            def userScript():
                from toil.common import Toil
                from toil.job import Job

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
                        '--mesosEndpoint=localhost:5050',
                        '--defaultMemory=10M',
                        '/data/jobstore']
            command = concat(pythonArgs, toilArgs)
            self.assertRaises(subprocess.CalledProcessError, leader.runOnAppliance, *command)

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
        Test whether auto-deployment works with a virtualenv in which jobs are defined in
        completely separate branches of the package hierarchy. Initially, auto-deployment did
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
                # noinspection PyUnresolvedReferences
                from toil_lib.foo import libraryJob

                from toil.common import Toil
                from toil.job import Job

                # noinspection PyUnusedLocal
                def job(job, disk='10M', cores=1, memory='10M'):
                    # Double the requirements to prevent chaining as chaining might hide problems
                    # in auto-deployment code.
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
            self.assertRaises(subprocess.CalledProcessError,
                              worker.runOnAppliance, 'test', '-f', '/data/foo.txt')
            leader.runOnAppliance('venv/bin/python',
                                  '-m', 'toil_script.bar',
                                  '--logDebug',
                                  '--batchSystem=mesos',
                                  '--mesosEndpoint=localhost:5050',
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
                from toil.common import Toil
                from toil.job import Job

                # A user-defined type, i.e. a type defined in the user script
                class X:
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
                    # translation from __main__ to foo.bar is a side effect of auto-deployment.
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
                                  '--mesosEndpoint=localhost:5050',
                                  '--defaultMemory=10M',
                                  '--defaultDisk=10M',
                                  '/data/jobstore')

    def testDeferralWithConcurrentEncapsulation(self):
        """
        Ensure that the following DAG succeeds:

                      ┌───────────┐
                      │ Root (W1) │
                      └───────────┘
                            │
                 ┌──────────┴─────────┐
                 ▼                    ▼
        ┌────────────────┐ ┌────────────────────┐
        │ Deferring (W2) │ │ Encapsulating (W3) │═══════════════╗
        └────────────────┘ └────────────────────┘               ║
                                      │                         ║
                                      ▼                         ▼
                            ┌───────────────────┐      ┌────────────────┐
                            │ Encapsulated (W3) │      │ Follow-on (W6) │
                            └───────────────────┘      └────────────────┘
                                      │                         │
                              ┌───────┴────────┐                │
                              ▼                ▼                ▼
                      ┌──────────────┐ ┌──────────────┐ ┌──────────────┐
                      │ Dummy 1 (W4) │ │ Dummy 2 (W5) │ │  Last (W6)   │
                      └──────────────┘ └──────────────┘ └──────────────┘

        The Wn numbers denote the worker processes that a particular job is run in. `Deferring`
        adds a deferred function and then runs for a long time. The deferred function will be
        present in the cache state for the duration of `Deferred`. `Follow-on` is the generic Job
        instance that's added by encapsulating a job. It runs on the same worker node but in a
        separate worker process, as the first job in that worker. Because …

        1) it is the first job in its worker process (the user script has not been made available
        on the sys.path by a previous job in that worker) and

        2) it shares the cache state with the `Deferring` job and

        3) it is an instance of Job (and so does not introduce the user script to sys.path itself),

        … it might cause problems with deserializing a defered function defined in the user script.

        `Encapsulated` has two children to ensure that `Follow-on` is run in a separate worker.
        """
        with self._venvApplianceCluster() as (leader, worker):
            def userScript():
                from toil.common import Toil
                from toil.job import Job

                def root(rootJob):
                    def nullFile():
                        return rootJob.fileStore.jobStore.import_file('file:///dev/null')

                    startFile = nullFile()
                    endFile = nullFile()
                    rootJob.addChildJobFn(deferring, startFile, endFile)
                    encapsulatedJob = Job.wrapJobFn(encapsulated, startFile)
                    encapsulatedJob.addChildFn(dummy)
                    encapsulatedJob.addChildFn(dummy)
                    encapsulatingJob = encapsulatedJob.encapsulate()
                    rootJob.addChild(encapsulatingJob)
                    encapsulatingJob.addChildJobFn(last, endFile)

                def dummy():
                    pass

                def deferred():
                    pass

                # noinspection PyUnusedLocal
                def deferring(job, startFile, endFile):
                    job.defer(deferred)
                    job.fileStore.jobStore.delete_file(startFile)
                    timeout = time.time() + 10
                    while job.fileStore.jobStore.file_exists(endFile):
                        assert time.time() < timeout
                        time.sleep(1)

                def encapsulated(job, startFile):
                    timeout = time.time() + 10
                    while job.fileStore.jobStore.file_exists(startFile):
                        assert time.time() < timeout
                        time.sleep(1)

                def last(job, endFile):
                    job.fileStore.jobStore.delete_file(endFile)

                if __name__ == '__main__':
                    options = Job.Runner.getDefaultArgumentParser().parse_args()
                    with Toil(options) as toil:
                        rootJob = Job.wrapJobFn(root)
                        toil.start(rootJob)

            userScript = self._getScriptSource(userScript)

            leader.deployScript(path=self.sitePackages,
                                packagePath='foo.bar',
                                script=userScript)

            leader.runOnAppliance('venv/bin/python', '-m', 'foo.bar',
                                  '--logDebug',
                                  '--batchSystem=mesos',
                                  '--mesosEndpoint=localhost:5050',
                                  '--retryCount=0',
                                  '--defaultMemory=10M',
                                  '--defaultDisk=10M',
                                  '/data/jobstore')

    def testDeferralWithFailureAndEncapsulation(self):
        """
        Ensure that the following DAG succeeds:

                      ┌───────────┐
                      │ Root (W1) │
                      └───────────┘
                            │
                 ┌──────────┴─────────┐
                 ▼                    ▼
        ┌────────────────┐ ┌────────────────────┐
        │ Deferring (W2) │ │ Encapsulating (W3) │═══════════════════════╗
        └────────────────┘ └────────────────────┘                       ║
                                      │                                 ║
                                      ▼                                 ▼
                            ┌───────────────────┐              ┌────────────────┐
                            │ Encapsulated (W3) │════════════╗ │ Follow-on (W7) │
                            └───────────────────┘            ║ └────────────────┘
                                      │                      ║
                               ┌──────┴──────┐               ║
                               ▼             ▼               ▼
                        ┌────────────┐┌────────────┐ ┌──────────────┐
                        │ Dummy (W4) ││ Dummy (W5) │ │ Trigger (W6) │
                        └────────────┘└────────────┘ └──────────────┘

        `Trigger` causes `Deferring` to crash. `Follow-on` runs next, detects `Deferring`'s
        left-overs and runs the deferred function. `Follow-on` is an instance of `Job` and the
        first job in its worker process. This test ensures that despite these circumstances,
        the user script is loaded before the deferred functions defined in it are being run.

        `Encapsulated` has two children to ensure that `Follow-on` is run in a new worker. That's
        the only way to guarantee that the user script has not been loaded yet, which would cause
        the test to succeed coincidentally. We want to test that auto-deploying and loading of the
        user script are done properly *before* deferred functions are being run and before any
        jobs have been executed by that worker.
        """
        with self._venvApplianceCluster() as (leader, worker):
            def userScript():
                import os
                import time

                from toil.common import Toil
                from toil.job import Job

                TIMEOUT = 10

                def root(rootJob):
                    def nullFile():
                        return rootJob.fileStore.jobStore.import_file('file:///dev/null')

                    startFile = nullFile()
                    endFile = nullFile()

                    rootJob.addChildJobFn(deferring, startFile, endFile)
                    encapsulatedJob = Job.wrapJobFn(encapsulated, startFile)
                    encapsulatedJob.addChildFn(dummy)
                    encapsulatedJob.addChildFn(dummy)
                    encapsulatedJob.addFollowOnJobFn(trigger, endFile)
                    encapsulatingJob = encapsulatedJob.encapsulate()
                    rootJob.addChild(encapsulatingJob)

                def dummy():
                    pass

                def deferredFile(config):
                    """
                    Return path to a file at the root of the job store, exploiting the fact that
                    the job store is shared between leader and worker container.
                    """
                    prefix = 'file:'
                    locator = config.jobStore
                    assert locator.startswith(prefix)
                    return os.path.join(locator[len(prefix):], 'testDeferredFile')

                def deferred(deferredFilePath):
                    """
                    The deferred function that is supposed to run.
                    """
                    os.unlink(deferredFilePath)

                # noinspection PyUnusedLocal
                def deferring(job, startFile, endFile):
                    """
                    A job that adds the deferred function and then crashes once the `trigger` job
                    tells it to.
                    """
                    job.defer(deferred, deferredFile(job._config))
                    jobStore = job.fileStore.jobStore
                    jobStore.delete_file(startFile)
                    with jobStore.update_file_stream(endFile) as fH:
                        fH.write(str(os.getpid()))
                    timeout = time.time() + TIMEOUT
                    while jobStore.file_exists(endFile):
                        assert time.time() < timeout
                        time.sleep(1)
                    os.kill(os.getpid(), 9)

                def encapsulated(job, startFile):
                    """
                    A job that waits until the `deferring` job is running and waiting to be crashed.
                    """
                    timeout = time.time() + TIMEOUT
                    while job.fileStore.jobStore.file_exists(startFile):
                        assert time.time() < timeout
                        time.sleep(1)

                def trigger(job, endFile):
                    """
                    A job that determines the PID of the worker running the `deferring` job,
                    tells the `deferring` job to crash and then waits for the corresponding
                    worker process to end. By waiting we can be sure that the `follow-on` job
                    finds the left-overs of the `deferring` job.
                    """
                    import errno
                    jobStore = job.fileStore.jobStore
                    with jobStore.read_file_stream(endFile) as fH:
                        pid = int(fH.read())
                    os.kill(pid, 0)
                    jobStore.delete_file(endFile)
                    timeout = time.time() + TIMEOUT
                    while True:
                        try:
                            os.kill(pid, 0)
                        except OSError as e:
                            if e.errno == errno.ESRCH:
                                break
                            else:
                                raise
                        else:
                            assert time.time() < timeout
                            time.sleep(1)

                def tryUnlink(deferredFilePath):
                    try:
                        os.unlink(deferredFilePath)
                    except OSError as e:
                        if e.errno == errno.ENOENT:
                            pass
                        else:
                            raise

                if __name__ == '__main__':
                    import errno
                    options = Job.Runner.getDefaultArgumentParser().parse_args()
                    with Toil(options) as toil:
                        deferredFilePath = deferredFile(toil.config)
                        open(deferredFilePath, 'w').close()
                        try:
                            assert os.path.exists(deferredFilePath)
                            try:
                                toil.start(Job.wrapJobFn(root))
                            except FailedJobsException as e:
                                assert e.numberOfFailedJobs == 2 # `root` and `deferring`
                                assert not os.path.exists(deferredFilePath), \
                                    'Apparently, the deferred function did not run.'
                            else:
                                assert False, 'Workflow should not have succeeded.'
                        finally:
                            tryUnlink(deferredFilePath)

            userScript = self._getScriptSource(userScript)

            leader.deployScript(path=self.sitePackages,
                                packagePath='foo.bar',
                                script=userScript)

            leader.runOnAppliance('venv/bin/python', '-m', 'foo.bar',
                                  '--logDebug',
                                  '--batchSystem=mesos',
                                  '--mesosEndpoint=localhost:5050',
                                  '--retryCount=0',
                                  '--defaultMemory=10M',
                                  '--defaultDisk=10M',
                                  '/data/jobstore')
