import logging
import os
import pipes
import subprocess
from contextlib import contextmanager
from urlparse import urlparse
from uuid import uuid4

from bd2k.util.iterables import concat
from cgcloud.lib.test import CgcloudTestCase

from toil.test import integrative, ToilTest
from toil.version import version as toil_version, cgcloudVersion

log = logging.getLogger(__name__)


@integrative
class CGCloudProvisionerTest(ToilTest, CgcloudTestCase):
    """
    Tests Toil on a CGCloud-provisioned cluster in AWS. Uses the RNASeq integration test workflow
    from toil-scripts. Once case is testing a cluster with a fixed number of worker nodes,
    another one tests auto-scaling.
    """
    # Create new toil-box AMI before creating cluster. If False, you will almost certainly have
    # to set the CGCLOUD_NAMESPACE environment variable to run the tests in a namespace that has
    # a pre-existing image. Also keep in mind that this image will quickly get stale,
    # as the image contains the version of Toil currently checked out, including any uncommitted
    # changes.
    #
    createImage = True

    # Set this to False to skip the cluster creation and to deploy the latest version of the Toil
    # source distribution on the leader only. Keep in mind that workers will use whatever
    # version of Toil is installed in the image.
    #
    createCluster = True

    # Tear down the cluster and delete toil-box AMI when done. Set this to False if you want to
    # reuse the image for a subsequent test run with createImage set to False. If you do,
    # you will need to clean-up the AMI and the cluster manually.
    #
    cleanup = True

    # Whether to skip all `cgcloud` invocations and just log them instead.
    #
    dryRun = False

    # The number of samples to run the test workflow on
    #
    numSamples = 10

    # The number of workers in a static cluster, ignored by auto-scaling tests
    #
    numWorkers = 10

    # The path to PyCharm's debug egg. If set, it will be copied to the leader to aid in remote
    # debugging. Use the following fragment to instrument the code for remote debugging:
    #
    #    import sys as _sys
    #    _sys.path.append('/home/mesosbox/pycharm-debug.egg')
    #    import pydevd
    #    pydevd.settrace('127.0.0.1', port=21212, suspend=True, stdoutToServer=True,
    #                                 stderrToServer=True, trace_only_current_thread=False)
    #
    # On the machine running PyCharm, run `cgcloud ssh toil-leader -R 21212:localhost:21212` to
    # tunnel the debug client's connection back to the debug server running in PyCharm.
    #
    if True:
        debugEggPath = None
    else:
        debugEggPath = '/Applications/PyCharm 2016.1.app/Contents/debug-eggs/pycharm-debug.egg'

    # A pip-installable release of toil-scripts or a URL pointing to a source distribution.
    # Typically you would specify a GitHub archive URL of a specific branch, tag or commit. The
    # first path component of each tarball entry will be stripped.
    #
    if True:
        toilScripts = '2.1.0a1.dev455'
    else:
        toilScripts = 'https://api.github.com/repos/BD2KGenomics/toil-scripts/tarball/master'

    # The AWS region to run everything in
    #
    region = 'us-west-2'

    # The instance types to use for leader and workers
    #
    instanceType = 'm3.large'
    leaderInstanceType = instanceType

    @classmethod
    def setUpClass(cls):
        logging.basicConfig(level=logging.INFO)
        super(CGCloudProvisionerTest, cls).setUpClass()

    def setUp(self):
        super(CGCloudProvisionerTest, self).setUp()
        self.saved_cgcloud_plugins = os.environ.get('CGCLOUD_PLUGINS')
        os.environ['CGCLOUD_PLUGINS'] = 'cgcloud.toil'
        self._checkSourceDistribution()
        self.jobStore = 'aws:%s:toil-it-%s' % (self.region, uuid4())

    def tearDown(self):
        if self.saved_cgcloud_plugins is not None:
            os.environ['CGCLOUD_PLUGINS'] = self.saved_cgcloud_plugins
        super(CGCloudProvisionerTest, self).tearDown()

    @integrative
    def testStaticCluster(self):
        self._test(autoScaled=False)

    @integrative
    def testAutoScaledCluster(self):
        self._test(autoScaled=True)

    def _test(self, autoScaled=False):
        with self.cgcloudVenv():
            self._run('pip', 'install', 'cgcloud-toil==' + cgcloudVersion)
            if self.createImage:
                self._cgcloud('create', '-IT',
                              '--option', 'toil_sdists=%s[aws,mesos]' % self.sdistPath,
                              'toil-latest-box')
            try:
                if self.createCluster:
                    self._cgcloud('create-cluster',
                                  '--leader-instance-type=' + self.leaderInstanceType,
                                  '--instance-type=' + self.instanceType,
                                  '--num-workers=%i' % (0 if autoScaled else self.numWorkers),
                                  'toil')
                try:
                    # Update Toil unless we created a fresh image
                    if not self.createImage:
                        sdistName = os.path.basename(self.sdistPath)
                        self._rsync('-a', 'toil-leader', '-v', self.sdistPath, ':' + sdistName)
                        self._leader('sudo pip install --upgrade %s[aws,mesos]' % sdistName,
                                     admin=True)
                        self._leader('rm', sdistName, admin=True)
                    if self.debugEggPath:
                        self._rsync('toil-leader', '-v', self.debugEggPath, ':')
                    self._leader('virtualenv', '--system-site-packages', '~/venv')
                    toilScripts = urlparse(self.toilScripts)
                    if toilScripts.netloc:
                        self._leader('mkdir toil-scripts'
                                     '; curl -L ' + toilScripts.geturl() +
                                     '| tar -C toil-scripts -xvz --strip-components=1')
                        self._leader('PATH=~/venv/bin:$PATH make -C toil-scripts develop')
                    else:
                        version = toilScripts.path
                        self._leader('~/venv/bin/pip', 'install', 'toil-scripts==' + version)
                    toilOptions = ['--batchSystem=mesos',
                                   '--mesosMaster=mesos-master:5050',
                                   '--clean=always']
                    if autoScaled:
                        toilOptions.extend(['--provisioner=cgcloud',
                                            '--nodeType=' + self.instanceType,
                                            '--logDebug',
                                            '--scaleInterval=10'])
                    toilOptions = ' '.join(toilOptions)
                    self._leader('PATH=~/venv/bin:$PATH',
                                 'TOIL_SCRIPTS_TEST_NUM_SAMPLES=%i' % self.numSamples,
                                 'TOIL_SCRIPTS_TEST_TOIL_OPTIONS=' + pipes.quote(toilOptions),
                                 'TOIL_SCRIPTS_TEST_JOBSTORE=' + self.jobStore,
                                 'python', '-m', 'unittest', '-v',
                                 'toil_scripts.rnaseq_cgl.test.test_rnaseq_cgl.RNASeqCGLTest'
                                 '.test_manifest')
                finally:
                    if self.cleanup and self.createCluster:
                        self._cgcloud('terminate-cluster', 'toil')
            finally:
                if self.cleanup and self.createImage:
                    self._cgcloud('delete-image', 'toil-latest-box')

    @contextmanager
    def cgcloudVenv(self):
        path = self._createTempDir(purpose='cgcloud-venv')
        self._run('virtualenv', path)
        binPath = os.path.join(path, 'bin')
        path = os.environ['PATH']
        os.environ['PATH'] = os.pathsep.join(concat(binPath, path.split(os.pathsep)))
        try:
            yield
        finally:
            os.environ['PATH'] = path

    @property
    def sdistPath(self):
        return os.path.join(self._projectRootPath(), 'dist', 'toil-%s.tar.gz' % toil_version)

    def _run(self, *args, **kwargs):
        log.info('Running %r', args)
        try:
            capture = kwargs['capture']
        except KeyError:
            capture = False
        else:
            del kwargs['capture']
        if capture:
            return subprocess.check_output(args, **kwargs)
        else:
            subprocess.check_call(args, **kwargs)
            return None

    def _cgcloud(self, *args):
        if not self.dryRun:
            self._run('cgcloud', *args)

    sshOptions = ['-o', 'UserKnownHostsFile=/dev/null', '-o', 'StrictHostKeyChecking=no']

    def _ssh(self, role, *args, **kwargs):
        try:
            admin = kwargs['admin']
        except KeyError:
            admin = False
        else:
            del kwargs['admin']

        self._cgcloud(
            *filter(None, concat('ssh', '-a' if admin else None, role, self.sshOptions, args)))

    def _rsync(self, role, *args):
        self._cgcloud('rsync', '--ssh-opts=' + ' '.join(self.sshOptions), role, *args)

    def _leader(self, *args, **kwargs):
        self._ssh('toil-leader', *args, **kwargs)

    def _checkSourceDistribution(self):
        self.assertTrue(os.path.isfile(self.sdistPath),
                        'Cannot find source distribution of Toil (%s)' % self.sdistPath)
        excluded = set(self._run('git', 'ls-files', '--others', '-i', '--exclude-standard',
                                 capture=True,
                                 cwd=self._projectRootPath()).splitlines())
        dirty = self._run('find', '.', '-type', 'f', '-newer', self.sdistPath,
                          capture=True,
                          cwd=self._projectRootPath()).splitlines()
        self.assertTrue(all(path.startswith('./') for path in dirty))
        dirty = set(path[2:] for path in dirty if not path.startswith('./.git'))
        dirty.difference_update(excluded)
        self.assertFalse(dirty, "You need to run 'make sdist'. "
                                "Files newer than %s: %r" % (self.sdistPath, list(dirty)))
