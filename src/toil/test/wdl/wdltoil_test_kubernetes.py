import unittest
from uuid import uuid4

import pytest

from toil.provisioners import cluster_factory
from toil.test import integrative, slow
from toil.test.provisioners.clusterTest import AbstractClusterTest
from toil.test.wdl.wdltoil_test import (
    WDL_CONFORMANCE_TEST_COMMIT,
    WDL_CONFORMANCE_TEST_REPO,
)


@integrative
@slow
@pytest.mark.timeout(600)
class WDLKubernetesClusterTest(AbstractClusterTest):
    """
    Ensure WDL works on the Kubernetes batchsystem.
    """

    def __init__(self, name):
        super().__init__(name)
        self.clusterName = "wdl-integration-test-" + str(uuid4())
        # t2.medium is the minimum t2 instance that permits Kubernetes
        self.leaderNodeType = "t2.medium"
        self.instanceTypes = ["t2.medium"]
        self.clusterType = "kubernetes"

    def setUp(self) -> None:
        super().setUp()
        self.jobStore = f"aws:{self.awsRegion()}:wdl-test-{uuid4()}"

    def launchCluster(self) -> None:
        self.createClusterUtil(
            args=[
                "--leaderStorage",
                str(self.requestedLeaderStorage),
                "--nodeTypes",
                ",".join(self.instanceTypes),
                "-w",
                ",".join(self.numWorkers),
                "--nodeStorage",
                str(self.requestedLeaderStorage),
            ]
        )

    def test_wdl_kubernetes_cluster(self):
        """
        Test that a wdl workflow works on a kubernetes cluster. Launches a cluster with 1 worker. This runs a wdl
        workflow that performs an image pull on the worker.
        :return:
        """
        self.numWorkers = "1"
        self.requestedLeaderStorage = 30
        # create the cluster
        self.launchCluster()
        # get leader
        self.cluster = cluster_factory(
            provisioner="aws", zone=self.zone, clusterName=self.clusterName
        )
        self.leader = self.cluster.getLeader()

        wdl_dir = "wdl_conformance_tests"

        # get the wdl-conformance-tests repo to get WDL tasks to run
        self.sshUtil(
            [
                "bash",
                "-c",
                f"git clone {WDL_CONFORMANCE_TEST_REPO} {wdl_dir} && cd {wdl_dir} && git checkout {WDL_CONFORMANCE_TEST_COMMIT}",
            ]
        )

        # run on kubernetes batchsystem
        toil_options = ["--batchSystem=kubernetes", f"--jobstore={self.jobStore}"]

        # run WDL workflow that will run singularity
        test_options = [f"tests/md5sum/md5sum.wdl", f"tests/md5sum/md5sum.json"]
        self.sshUtil(
            [
                "bash",
                "-c",
                f"cd {wdl_dir} && toil-wdl-runner {' '.join(test_options)} {' '.join(toil_options)}",
            ]
        )


if __name__ == "__main__":
    unittest.main()  # run all tests
