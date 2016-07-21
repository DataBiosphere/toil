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
    Tests Toil on a Mesos cluster in AWS provisioned by CGCloud. Uses the RNASeq integration test
    workflow from toil-scripts.
    """
    # Whether to skip and log all cgcloud invocations.
    dryRun = False

    # Create new toil-box AMI before creating cluster. If False, you will almost certainly have
    # to set CGCLOUD_NAMESPACE to run the tests in a namespace that has a pre-existing image.
    # Also keep in mind that this image will quickly get stale, as the image will contain the
    # version of Toil currently checked out, even if it is dirty.
    createImage = True

    # Delete toil-box AMI when done.
    cleanup = True

    # A pip-installable release of toil-scripts or a URL pointing to a source distribution.
    # Typically you would specify a GitHub archive URL of a specific branch, tag or commit. For
    # example, https://api.github.com/repos/BD2KGenomics/toil-scripts/tarball/master. The first
    # path component of each tarball entry will be stripped.
    if True:
        toilScripts = '2.1.0a1.dev455'
    else:
        toilScripts = 'https://api.github.com/repos/BD2KGenomics/toil-scripts/tarball/master'

    toilOptions = ['--batchSystem=mesos',
                   '--mesosMaster=mesos-master:5050',
                   '--logDebug',
                   '--clean=always']

    region = 'us-west-2'

    @classmethod
    def setUpClass(cls):
        logging.basicConfig(level=logging.INFO)
        super(CGCloudProvisionerTest, cls).setUpClass()

    def setUp(self):
        super(CGCloudProvisionerTest, self).setUp()
        self.saved_cgcloud_plugins = os.environ.get('CGCLOUD_PLUGINS')
        os.environ['CGCLOUD_PLUGINS'] = 'cgcloud.toil'
        self.assertTrue(os.path.isfile(self.sdistPath),
                        'Cannot find source distribution of Toil (%s)' % self.sdistPath)
        self.jobStore = 'aws:%s:toil-it-%s' % (self.region, uuid4())

    def tearDown(self):
        if self.saved_cgcloud_plugins is not None:
            os.environ['CGCLOUD_PLUGINS'] = self.saved_cgcloud_plugins
        super(CGCloudProvisionerTest, self).tearDown()

    @integrative
    def test(self):
        with self.cgcloudVenv():
            self._run('pip', 'install', 'cgcloud-toil==' + cgcloudVersion)
            if self.createImage:
                self._cgcloud('create', '-IT',
                              '--option', 'toil_sdists=%s[aws,mesos]' % self.sdistPath,
                              'toil-latest-box')
            try:
                self._cgcloud('create-cluster',
                              '--leader-instance-type=m3.large',
                              '--instance-type=m3.large',
                              '--num-workers=10',
                              'toil')
                try:
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
                    toilOptions = ' '.join(self.toilOptions)
                    self._leader('PATH=~/venv/bin:$PATH',
                                 'TOIL_SCRIPTS_TEST_NUM_SAMPLES=10',
                                 'TOIL_SCRIPTS_TEST_TOIL_OPTIONS=' + pipes.quote(toilOptions),
                                 'TOIL_SCRIPTS_TEST_JOBSTORE=' + self.jobStore,
                                 'python', '-m', 'unittest', '-v',
                                 'toil_scripts.rnaseq_cgl.test.test_rnaseq_cgl.RNASeqCGLTest'
                                 '.test_manifest')
                finally:
                    self._cgcloud('terminate-cluster', 'toil')
            finally:
                if self.cleanup:
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
        if kwargs.get('capture', False):
            return subprocess.check_output(args)
        else:
            subprocess.check_call(args)
            return None

    def _cgcloud(self, *args):
        if not self.dryRun:
            self._run('cgcloud', *args)

    def _ssh(self, role, *args):
        self._cgcloud('ssh', role,
                      '-o', 'UserKnownHostsFile=/dev/null',
                      '-o', 'StrictHostKeyChecking=no', *args)

    def _leader(self, *args):
        self._ssh('toil-leader', *args)
