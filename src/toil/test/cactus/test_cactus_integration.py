import os
import uuid

from toil.provisioners import cluster_factory
from toil.test.provisioners.clusterTest import AbstractClusterTest


class CactusIntegrationTest(AbstractClusterTest):
    """
    Run the Cactus Integration test on a Kubernetes AWS cluster
    """

    def __init__(self, methodName):
        super().__init__(methodName=methodName)
        self.clusterName = "cactus-test-" + str(uuid.uuid4())
        self.leaderNodeType = "t2.medium"
        self.clusterType = "kubernetes"

    def setUp(self):
        super().setUp()
        self.jobStore = f"aws:{self.awsRegion()}:cluster-{uuid.uuid4()}"

    def test_cactus_integration(self):
        # Make a cluster with worker nodes
        self.createClusterUtil(args=["--nodeTypes=t2.xlarge", "-w=1-3"])
        # get the leader so we know the IP address - we don't need to wait since create cluster
        # already ensures the leader is running
        self.cluster = cluster_factory(
            provisioner="aws", zone=self.zone, clusterName=self.clusterName
        )
        self.leader = self.cluster.getLeader()

        CACTUS_COMMIT_SHA = os.environ["CACTUS_COMMIT_SHA"] or "f5adf4013326322ae58ef1eccb8409b71d761583"  # default cactus commit

        # command to install and run cactus on the cluster
        cactus_command = ("python -m virtualenv --system-site-packages venv && "
                          ". venv/bin/activate && "
                          "git clone https://github.com/ComparativeGenomicsToolkit/cactus.git --recursive && "
                          "cd cactus && "
                          "git fetch origin && "
                          f"git checkout {CACTUS_COMMIT_SHA} && "
                          "git submodule update --init --recursive && "
                          "pip install --upgrade 'setuptools' pip && "
                          "pip install --upgrade . && "
                          "pip install --upgrade numpy psutil && "
                          "time cactus --batchSystem kubernetes --retryCount=3 "
                          f"--consCores 2 --binariesMode singularity --clean always {self.jobStore} "
                          "examples/evolverMammals.txt examples/evolverMammals.hal --root mr --defaultDisk 8G --logDebug")

        # run cactus
        self.sshUtil(
            [
                "bash",
                "-c",
                cactus_command
            ]
        )

