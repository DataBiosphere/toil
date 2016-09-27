# Copyright (C) 2015-2016 Regents of the University of California
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

import logging
import os
import pipes
import subprocess
from abc import abstractmethod, ABCMeta
from urlparse import urlparse
from uuid import uuid4

from bd2k.util.iterables import concat
from cgcloud.lib.test import CgcloudTestCase

from toil.test import integrative, ToilTest
from toil.version import version as toil_version, cgcloudVersion

log = logging.getLogger(__name__)


@integrative
class AbstractCGCloudProvisionerTest(ToilTest, CgcloudTestCase):
    __metaclass__ = ABCMeta
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

    # The number of workers in a static cluster, the maximum number of prepemptable and
    # non-preemptable workers each in an auto-scaled cluster.
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

    # The instance types to use for leader and workers
    #
    instanceType = 'm3.large'
    leaderInstanceType = instanceType

    @classmethod
    def setUpClass(cls):
        logging.basicConfig(level=logging.INFO)
        super(AbstractCGCloudProvisionerTest, cls).setUpClass()
        cls.saved_cgcloud_plugins = os.environ.get('CGCLOUD_PLUGINS')
        os.environ['CGCLOUD_PLUGINS'] = 'cgcloud.toil'
        cls.sdistPath = cls._getSourceDistribution()
        path = cls._createTempDirEx('cgcloud-venv')
        cls._run('virtualenv', path)
        binPath = os.path.join(path, 'bin')
        cls.oldPath = os.environ['PATH']
        os.environ['PATH'] = os.pathsep.join(concat(binPath, cls.oldPath.split(os.pathsep)))
        cls._run('pip', 'install', 'cgcloud-toil==' + cgcloudVersion)
        if cls.createImage:
            cls._cgcloud('create', '-IT',
                         '--option', 'toil_sdists=%s[aws,mesos]' % cls.sdistPath,
                         'toil-latest-box')

    @classmethod
    def tearDownClass(cls):
        if cls.cleanup and cls.createImage:
            cls._cgcloud('delete-image', 'toil-latest-box')
        os.environ['PATH'] = cls.oldPath
        super(AbstractCGCloudProvisionerTest, cls).tearDownClass()

    def setUp(self):
        super(AbstractCGCloudProvisionerTest, self).setUp()
        self.jobStore = 'aws:%s:toil-it-%s' % (self.awsRegion(), uuid4())

    def tearDown(self):
        if self.saved_cgcloud_plugins is not None:
            os.environ['CGCLOUD_PLUGINS'] = self.saved_cgcloud_plugins
        super(AbstractCGCloudProvisionerTest, self).tearDown()

    def _test(self,
              autoScaled=False,
              spotInstances=False,
              spotBid = '0.133',
              slackPreference=None):
        self.assertTrue(not spotInstances or autoScaled,
                        'This test does not support a static cluster of spot instances.')
        if self.createCluster:
            self._cgcloud('create-cluster',
                          '--leader-instance-type=' + self.leaderInstanceType,
                          '--instance-type=' + self.instanceType,
                          '--num-workers=%i' % (0 if autoScaled else self.numWorkers),
                          'toil')
        try:
            # Update Toil unless we created a fresh image during setup
            if not self.createImage:
                sdistName = os.path.basename(self.sdistPath)
                self._rsync('-a', 'toil-leader', '-v', self.sdistPath, ':' + sdistName)
                self._leader('sudo pip install --upgrade %s[aws,mesos]' % sdistName,
                             admin=True)
                self._leader('rm', sdistName, admin=True)
            if self.debugEggPath:
                self._rsync('toil-leader', '-v', self.debugEggPath, ':')
            self._leader('virtualenv', '--system-site-packages', '~/venv')

            self._getScript()

            toilOptions = ['--batchSystem=mesos',
                           '--mesosMaster=mesos-master:5050',
                           '--clean=always',
                           '--retryCount=2']
            if autoScaled:
                toilOptions.extend(['--provisioner=cgcloud',
                                    '--nodeType=' + self.instanceType,
                                    '--maxNodes=%s' % self.numWorkers,
                                    '--logDebug'])
            if spotInstances:
                toilOptions.extend([
                    '--preemptableNodeType=%s:%s' % (self.instanceType, spotBid),
                    # The RNASeq pipeline does not specify a preemptability requirement so we
                    # need to specify a default, otherwise jobs would never get scheduled.
                    '--defaultPreemptable',
                    '--maxPreemptableNodes=%s' % self.numWorkers])

            if slackPreference:
                toilOptions.extend(['--slackPreemptablePreference', slackPreference])

            self._runScript(toilOptions)

        finally:
            if self.cleanup and self.createCluster:
                self._cgcloud('terminate-cluster', 'toil')

    @abstractmethod
    def _getScript(self):
        """
        Download the test script needed by the inheriting unit test class.
        """
        raise NotImplementedError()

    @abstractmethod
    def _runScript(self, toilOptions):
        """
        Modify the provided Toil options to suit the test Toil script, then run the script with those arguments.

        :param toilOptions: List of Toil command line arguments. This list may need to be modified to suit
            the test script's requirements.
        """
        raise NotImplementedError()

    @classmethod
    def _cgcloud(cls, *args):
        if not cls.dryRun:
            cls._run('cgcloud', *args)

    sshOptions = ['-o', 'UserKnownHostsFile=/dev/null', '-o', 'StrictHostKeyChecking=no']

    @classmethod
    def _ssh(cls, role, *args, **kwargs):
        try:
            admin = kwargs['admin']
        except KeyError:
            admin = False
        else:
            del kwargs['admin']

        cls._cgcloud(
            *filter(None, concat('ssh', '-a' if admin else None, role, cls.sshOptions, args)))

    @classmethod
    def _rsync(cls, role, *args):
        cls._cgcloud('rsync', '--ssh-opts=' + ' '.join(cls.sshOptions), role, *args)

    @classmethod
    def _leader(cls, *args, **kwargs):
        cls._ssh('toil-leader', *args, **kwargs)


class CGCloudRNASeqTest(AbstractCGCloudProvisionerTest):
    def __init__(self, name):
        super(CGCloudRNASeqTest, self).__init__(name)
        # The number of samples to run the test workflow on
        #
        self.numSamples = 10

    def _getScript(self):
        toilScripts = urlparse(self.toilScripts)
        if toilScripts.netloc:
            self._leader('mkdir toil-scripts'
                         '; curl -L ' + toilScripts.geturl() +
                         '| tar -C toil-scripts -xvz --strip-components=1')
            self._leader('PATH=~/venv/bin:$PATH make -C toil-scripts develop')
        else:
            version = toilScripts.path
            self._leader('~/venv/bin/pip', 'install', 'toil-scripts==' + version)

    def _runScript(self, toilOptions):
        toilOptions = ' '.join(toilOptions)
        self._leader('PATH=~/venv/bin:$PATH',
                     'TOIL_SCRIPTS_TEST_NUM_SAMPLES=%i' % self.numSamples,
                     'TOIL_SCRIPTS_TEST_TOIL_OPTIONS=' + pipes.quote(toilOptions),
                     'TOIL_SCRIPTS_TEST_JOBSTORE=' + self.jobStore,
                     'python', '-m', 'unittest', '-v',
                     'toil_scripts.rnaseq_cgl.test.test_rnaseq_cgl.RNASeqCGLTest'
                     '.test_manifest')

    @integrative
    def testStaticCluster(self):
        self._test(autoScaled=False)

    @integrative
    def testAutoScaledCluster(self):
        self._test(autoScaled=True)

    @integrative
    def testAutoScaledSpotCluster(self):
        self._test(autoScaled=True, spotInstances=True)

    @integrative
    def testAutoScaledSpotClusterWithLowSlack(self):
        # the spot market should never fulfill a spot bid of one hundredth of
        # one cent for m3.large (avg bid price ~$0.02)
        self._test(autoScaled=True,
                   spotInstances=True,
                   spotBid="0.0001",
                   slackPreference=0.0)
        # slack preference of 0.0 means that we immediately roll over all
        # unfulfilled preemptable requests to non-preemptable nodes

    @integrative
    def testAutoScaledSpotClusterWithMediumSlack(self):
        # the spot market should never fulfill a spot bid of one hundredth of
        # one cent for m3.large (avg bid price ~$0.02)
        self._test(autoScaled=True,
                   spotInstances=True,
                   spotBid="0.0001",
                   slackPreference=0.5)
        # slack preference of 0.5 means we only reissue half of unfulfilled
        # preemptable requests as non-preemptable requests
