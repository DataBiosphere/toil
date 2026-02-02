# Copyright (C) 2015-2021 Regents of the University of California
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
import collections
import os
import random
from collections.abc import Callable
from pathlib import Path
from typing import Any, Callable, NoReturn, cast

import pytest
from pytest_subtests import SubTests

from toil.common import Toil
from toil.exceptions import FailedJobsException
from toil.job import (
    FunctionWrappingJob,
    Job,
    JobFunctionWrappingJob,
    JobGraphDeadlockException,
    Promise,
    ServiceHostJob,
)
from toil.lib.misc import FileDescriptorOrPath
from toil.test import pslow as slow


class TestJob:
    """Tests the job class."""

    @slow
    @pytest.mark.slow
    def testStatic(self, tmp_path: Path) -> None:
        r"""
        Create a DAG of jobs non-dynamically and run it. DAG is::

            A -> F
            \-------
            B -> D  \
             \       \
              ------- C -> E

        Follow on is marked by ``->``
        """
        outFile = tmp_path / "out"
        # Create the jobs
        A = Job.wrapFn(fn1Test, "A", outFile)
        B = Job.wrapFn(fn1Test, A.rv(), outFile)
        C = Job.wrapFn(fn1Test, B.rv(), outFile)
        D = Job.wrapFn(fn1Test, C.rv(), outFile)
        E = Job.wrapFn(fn1Test, D.rv(), outFile)
        F = Job.wrapFn(fn1Test, E.rv(), outFile)
        # Connect them into a workflow
        A.addChild(B)
        A.addChild(C)
        B.addChild(C)
        B.addFollowOn(E)
        C.addFollowOn(D)
        A.addFollowOn(F)

        # Create the runner for the workflow.
        options = Job.Runner.getDefaultOptions(tmp_path / "jobstore")
        options.logLevel = "INFO"
        options.retryCount = 100
        options.badWorker = 0.5
        options.badWorkerFailInterval = 0.01
        # Run the workflow, the return value being the number of failed jobs
        Job.Runner.startToil(A, options)

        # Check output
        assert open(outFile).readline() == "ABCDEFG"

    def testStatic2(self, tmp_path: Path) -> None:
        r"""
        Create a DAG of jobs non-dynamically and run it. DAG is::

            A -> F
            \-------
            B -> D  \
             \       \
              ------- C -> E

        Follow on is marked by ``->``
        """
        outFile = tmp_path / "out"

        # Create the jobs
        A = Job.wrapFn(fn1Test, "A", outFile)
        B = Job.wrapFn(fn1Test, A.rv(), outFile)
        C = Job.wrapFn(fn1Test, B.rv(), outFile)
        D = Job.wrapFn(fn1Test, C.rv(), outFile)

        # Connect them into a workflow
        A.addChild(B)
        A.addFollowOn(C)
        C.addChild(D)

        # Create the runner for the workflow.
        options = Job.Runner.getDefaultOptions(tmp_path / "jobstore")
        options.logLevel = "INFO"
        options.retryCount = 100
        options.badWorker = 0.5
        options.badWorkerFailInterval = 0.01
        # Run the workflow, the return value being the number of failed jobs
        Job.Runner.startToil(A, options)

        # Check output
        assert open(outFile).readline() == "ABCDE"

    @slow
    @pytest.mark.slow
    def testTrivialDAGConsistency(self, tmp_path: Path) -> None:
        options = Job.Runner.getDefaultOptions(tmp_path / "jobStore")
        options.clean = "always"
        options.logLevel = "debug"
        i = Job.wrapJobFn(trivialParent)
        with Toil(options) as toil:
            with pytest.raises(FailedJobsException):
                toil.start(i)

    @pytest.mark.timeout(300)
    def testDAGConsistency(self, tmp_path: Path) -> None:
        options = Job.Runner.getDefaultOptions(tmp_path / "jobStore")
        options.clean = "always"
        options.logLevel = "debug"
        i = Job.wrapJobFn(parent)
        with Toil(options) as toil:
            with pytest.raises(FailedJobsException):
                toil.start(i)

    @slow
    @pytest.mark.slow
    def testSiblingDAGConsistency(self, tmp_path: Path) -> None:
        """
        Slightly more complex case. The stranded job's predecessors are siblings instead of
        parent/child.
        """
        options = Job.Runner.getDefaultOptions(tmp_path / "jobStore")
        options.clean = "always"
        options.logLevel = "debug"
        i = Job.wrapJobFn(diamond)
        with Toil(options) as toil:
            with pytest.raises(FailedJobsException):
                toil.start(i)

    def testDeadlockDetectionStatic(self) -> None:
        """
        Test cycle detection on small example graphs.
        """
        # Test 1: Simple 3-node graph with a child cycle: 0 -> 1 -> 2 -> 0
        childEdges: set[tuple[int, int]] = {(0, 1), (1, 2)}
        followOnEdges: set[tuple[int, int]] = set()
        # Valid DAG should pass
        rootJob = self.makeJobGraph(3, childEdges, followOnEdges, None, False)
        rootJob.checkJobGraphAcylic()
        rootJob.checkJobGraphConnected()
        # Adding edge (2, 0) creates cycle
        childEdges.add((2, 0))
        with pytest.raises(JobGraphDeadlockException):
            self.makeJobGraph(3, childEdges, followOnEdges, None, False).checkJobGraphAcylic()

        # Test 2: Self-loop (0 -> 0)
        childEdges = {(0, 1), (0, 0)}
        with pytest.raises(JobGraphDeadlockException):
            self.makeJobGraph(2, childEdges, followOnEdges, None, False).checkJobGraphAcylic()

        # Test 3: Multiple roots detection (nodes 0 and 2 are both roots)
        childEdges = {(0, 1), (2, 1)}  # Both 0 and 2 point to 1, neither has incoming edges
        with pytest.raises(JobGraphDeadlockException):
            self.makeJobGraph(3, childEdges, followOnEdges, None, False).checkJobGraphConnected()

        # Test 4: Follow-on creating a cycle
        # Graph: 0 -> 1, with follow-on 1 -> 0
        childEdges = {(0, 1)}
        followOnEdges = {(1, 0)}
        with pytest.raises(JobGraphDeadlockException):
            self.makeJobGraph(2, childEdges, followOnEdges, None, False).checkJobGraphAcylic()

        # Test 5: Self follow-on
        childEdges = {(0, 1)}
        followOnEdges = {(0, 0)}
        with pytest.raises(JobGraphDeadlockException):
            self.makeJobGraph(2, childEdges, followOnEdges, None, False).checkJobGraphAcylic()

        # Test 6: Larger graph - 5 nodes in a chain, add back-edge
        childEdges = {(0, 1), (1, 2), (2, 3), (3, 4)}
        followOnEdges = set()
        rootJob = self.makeJobGraph(5, childEdges, followOnEdges, None, False)
        rootJob.checkJobGraphAcylic()
        # Add back-edge from 4 to 2
        childEdges.add((4, 2))
        with pytest.raises(JobGraphDeadlockException):
            self.makeJobGraph(5, childEdges, followOnEdges, None, False).checkJobGraphAcylic()

    # We have this marked as xfail but allow it to pass also, to try and
    # collect failing cases without breaking unrelated PRs.
    # TODO: In the future, go through the CI logs and find the cases that
    # manage to fail. No failure was found in many iterations locally.
    @pytest.mark.xfail(
        reason="Flaky test - see https://github.com/DataBiosphere/toil/issues/5354",
        strict=False,
    )
    def testDeadlockDetection(self) -> None:
        """
        Randomly generate job graphs with various types of cycle in them and
        check they cause an exception properly. Also check that multiple roots
        causes a deadlock exception.
        """
        for test in range(10):
            # Make a random DAG for the set of child edges
            nodeNumber = random.choice(range(2, 20))
            childEdges = self.makeRandomDAG(nodeNumber)
            # Get an adjacency list representation and check is acyclic
            adjacencyList = self.getAdjacencyList(nodeNumber, childEdges)
            assert self.isAcyclic(adjacencyList)

            # Add in follow-on edges - these are returned as a list, and as a set of augmented
            # edges in the adjacency list
            # edges in the adjacency list
            followOnEdges = self.addRandomFollowOnEdges(adjacencyList)
            assert self.isAcyclic(adjacencyList)
            # Make the job graph
            rootJob = self.makeJobGraph(nodeNumber, childEdges, followOnEdges, None)
            rootJob.checkJobGraphAcylic()  # This should not throw an exception
            rootJob.checkJobGraphConnected()  # Nor this
            # Check root detection explicitly
            assert rootJob.getRootJobs() == {rootJob}

            # Test making multiple roots
            childEdges2 = childEdges.copy()
            childEdges2.add(
                (nodeNumber, 1)
            )  # This creates an extra root at "nodeNumber"
            rootJob2 = self.makeJobGraph(
                nodeNumber + 1, childEdges2, followOnEdges, None, False
            )
            self._assertDeadlockDetected(
                rootJob2.checkJobGraphConnected,
                "multiple roots",
                nodeNumber + 1,
                childEdges2,
                followOnEdges,
            )

            def checkChildEdgeCycleDetection(fNode: int, tNode: int) -> None:
                childEdges.add((fNode, tNode))  # Create a cycle
                adjacencyList[fNode].add(tNode)
                assert not self.isAcyclic(adjacencyList), (
                    f"isAcyclic incorrectly returned True after adding cycle edge "
                    f"({fNode}, {tNode}). nodeNumber={nodeNumber}, "
                    f"childEdges={childEdges}, followOnEdges={followOnEdges}"
                )
                self._assertDeadlockDetected(
                    lambda: self.makeJobGraph(
                        nodeNumber, childEdges, followOnEdges, None
                    ).checkJobGraphAcylic(),
                    f"child edge cycle ({fNode}, {tNode})",
                    nodeNumber,
                    childEdges,
                    followOnEdges,
                )
                # Remove the edges
                childEdges.remove((fNode, tNode))
                adjacencyList[fNode].remove(tNode)
                # Check is now acyclic again
                self.makeJobGraph(
                    nodeNumber, childEdges, followOnEdges, None, False
                ).checkJobGraphAcylic()

            def checkFollowOnEdgeCycleDetection(fNode: int, tNode: int) -> None:
                followOnEdges.add((fNode, tNode))  # Create a cycle
                self._assertDeadlockDetected(
                    lambda: self.makeJobGraph(
                        nodeNumber, childEdges, followOnEdges, None, False
                    ).checkJobGraphAcylic(),
                    f"follow-on edge cycle ({fNode}, {tNode})",
                    nodeNumber,
                    childEdges,
                    followOnEdges,
                )
                # Remove the edges
                followOnEdges.remove((fNode, tNode))
                # Check is now acyclic again
                self.makeJobGraph(
                    nodeNumber, childEdges, followOnEdges, None, False
                ).checkJobGraphAcylic()

            # Now try adding edges that create a cycle

            # Pick a random existing order relationship
            fNode, tNode = self.getRandomEdge(nodeNumber)
            while tNode not in self.reachable(fNode, adjacencyList):
                fNode, tNode = self.getRandomEdge(nodeNumber)

            # Try creating a cycle of child edges
            checkChildEdgeCycleDetection(tNode, fNode)

            # Try adding a self child edge
            node = random.choice(range(nodeNumber))
            checkChildEdgeCycleDetection(node, node)

            # Try adding a follow on edge from a descendant to an ancestor
            checkFollowOnEdgeCycleDetection(tNode, fNode)

            # Try adding a self follow on edge
            checkFollowOnEdgeCycleDetection(node, node)

            # Try adding a follow on edge between two nodes with shared descendants
            fNode, tNode = self.getRandomEdge(nodeNumber)
            if (
                len(
                    self.reachable(tNode, adjacencyList).intersection(
                        self.reachable(fNode, adjacencyList)
                    )
                )
                > 0
                and (fNode, tNode) not in childEdges
                and (fNode, tNode) not in followOnEdges
            ):
                checkFollowOnEdgeCycleDetection(fNode, tNode)

    def _assertDeadlockDetected(
        self,
        check_fn: Callable[[], None],
        description: str,
        nodeNumber: int,
        childEdges: set[tuple[int, int]],
        followOnEdges: set[tuple[int, int]],
    ) -> None:
        """
        Assert that check_fn raises JobGraphDeadlockException.

        If it doesn't, provide detailed graph info for reproduction.
        """
        try:
            check_fn()
        except JobGraphDeadlockException:
            return  # Expected
        pytest.fail(
            f"JobGraphDeadlockException not raised for {description}. "
            f"Graph info for reproduction: nodeNumber={nodeNumber}, "
            f"childEdges={repr(childEdges)}, followOnEdges={repr(followOnEdges)}"
        )

    @slow
    @pytest.mark.slow
    def testNewCheckpointIsLeafVertexNonRootCase(
        self, tmp_path: Path, subtests: SubTests
    ) -> None:
        """
        Test for issue #1465: Detection of checkpoint jobs that are not leaf vertices
        identifies leaf vertices incorrectly

        Test verification of new checkpoint jobs being leaf vertices,
        starting with the following baseline workflow::

            Parent
              |
            Child # Checkpoint=True

        """

        def createWorkflow() -> tuple[Job, FunctionWrappingJob]:
            rootJob = Job.wrapJobFn(simpleJobFn, "Parent")
            childCheckpointJob = rootJob.addChildJobFn(
                simpleJobFn, "Child", checkpoint=True
            )
            return rootJob, childCheckpointJob

        self.runNewCheckpointIsLeafVertexTest(tmp_path, subtests, createWorkflow)

    @slow
    @pytest.mark.slow
    def testNewCheckpointIsLeafVertexRootCase(
        self, tmp_path: Path, subtests: SubTests
    ) -> None:
        """
        Test for issue #1466: Detection of checkpoint jobs that are not leaf vertices
                              omits the workflow root job

        Test verification of a new checkpoint job being leaf vertex,
        starting with a baseline workflow of a single, root job::

            Root # Checkpoint=True

        """

        def createWorkflow() -> tuple[Job, Job]:
            rootJob = Job.wrapJobFn(simpleJobFn, "Root", checkpoint=True)
            return rootJob, rootJob

        self.runNewCheckpointIsLeafVertexTest(tmp_path, subtests, createWorkflow)

    def runNewCheckpointIsLeafVertexTest(
        self,
        tmp_path: Path,
        subtests: SubTests,
        createWorkflowFn: Callable[[], tuple[Job, Job]],
    ) -> None:
        """
        Test verification that a checkpoint job is a leaf vertex using both
        valid and invalid cases.

        :param createWorkflowFn: function to create and new workflow and return a tuple of:

                                 0) the workflow root job
                                 1) a checkpoint job to test within the workflow

        """

        with subtests.test(msg="Test checkpoint job that is a leaf vertex"):
            sub_tmp_path1 = tmp_path / "1"
            sub_tmp_path1.mkdir()
            self.runCheckpointVertexTest(
                *createWorkflowFn(), tmp_path=sub_tmp_path1, expectedException=None
            )

        with subtests.test(
            msg="Test checkpoint job that is not a leaf vertex due to the presence of a service"
        ):
            sub_tmp_path2 = tmp_path / "2"
            sub_tmp_path2.mkdir()
            self.runCheckpointVertexTest(
                *createWorkflowFn(),
                tmp_path=sub_tmp_path2,
                checkpointJobService=TrivialService("LeafTestService"),
                expectedException=JobGraphDeadlockException,
            )

        with subtests.test(
            msg="Test checkpoint job that is not a leaf vertex due to the presence of a child job"
        ):
            sub_tmp_path3 = tmp_path / "3"
            sub_tmp_path3.mkdir()
            self.runCheckpointVertexTest(
                *createWorkflowFn(),
                tmp_path=sub_tmp_path3,
                checkpointJobChild=Job.wrapJobFn(simpleJobFn, "LeafTestChild"),
                expectedException=JobGraphDeadlockException,
            )

        with subtests.test(
            msg="Test checkpoint job that is not a leaf vertex due to the presence of a follow-on job"
        ):
            sub_tmp_path4 = tmp_path / "4"
            sub_tmp_path4.mkdir()
            self.runCheckpointVertexTest(
                *createWorkflowFn(),
                tmp_path=sub_tmp_path4,
                checkpointJobFollowOn=Job.wrapJobFn(simpleJobFn, "LeafTestFollowOn"),
                expectedException=JobGraphDeadlockException,
            )

    def runCheckpointVertexTest(
        self,
        workflowRootJob: Job,
        checkpointJob: Job,
        tmp_path: Path,
        checkpointJobService: Job.Service | None = None,
        checkpointJobChild: Job | None = None,
        checkpointJobFollowOn: Job | None = None,
        expectedException: type[Exception] | None = None,
    ) -> None:
        """
        Modifies the checkpoint job according to the given parameters
        then runs the workflow, checking for the expected exception, if any.
        """

        assert checkpointJob.checkpoint

        if checkpointJobService is not None:
            checkpointJob.addService(checkpointJobService)
        if checkpointJobChild is not None:
            checkpointJob.addChild(checkpointJobChild)
        if checkpointJobFollowOn is not None:
            checkpointJob.addFollowOn(checkpointJobFollowOn)

        # Run the workflow and check for the expected behavior
        options = Job.Runner.getDefaultOptions(tmp_path / "jobstore")
        options.logLevel = "INFO"
        if expectedException is None:
            Job.Runner.startToil(workflowRootJob, options)
        else:
            with pytest.raises(expectedException):
                Job.Runner.startToil(workflowRootJob, options)

    @slow
    @pytest.mark.slow
    def testEvaluatingRandomDAG(self, tmp_path: Path) -> None:
        """
        Randomly generate test input then check that the job graph can be
        run successfully, using the existence of promises
        to validate the run.
        """
        jobStore = tmp_path / "jobstore"
        for test in range(5):
            # Temporary file
            tempDir = tmp_path / f"tempDir{test}"
            tempDir.mkdir()
            # Make a random DAG for the set of child edges
            nodeNumber = random.choice(range(2, 8))
            childEdges = self.makeRandomDAG(nodeNumber)
            # Get an adjacency list representation and check is acyclic
            adjacencyList = self.getAdjacencyList(nodeNumber, childEdges)
            assert self.isAcyclic(adjacencyList)
            # Add in follow on edges - these are returned as a list, and as a set of augmented
            # edges in the adjacency list
            followOnEdges = self.addRandomFollowOnEdges(adjacencyList)
            assert self.isAcyclic(adjacencyList)
            # Make the job graph
            rootJob = self.makeJobGraph(nodeNumber, childEdges, followOnEdges, tempDir)
            # Run the job  graph
            options = Job.Runner.getDefaultOptions("%s.%i" % (jobStore, test))
            options.logLevel = "DEBUG"
            options.retryCount = 1
            options.badWorker = 0.25
            options.badWorkerFailInterval = 0.01
            # Because we're going to be killing the services all the time for
            # restarts, make sure they are paying attention.
            options.servicePollingInterval = 1

            # Now actually run the workflow
            try:
                with Toil(options) as toil:
                    toil.start(rootJob)
                numberOfFailedJobs = 0
            except FailedJobsException as e:
                numberOfFailedJobs = e.numberOfFailedJobs

            # Restart until successful or failed
            totalTrys = 1
            options.restart = True
            while numberOfFailedJobs != 0:
                try:
                    with Toil(options) as toil:
                        toil.restart()
                    numberOfFailedJobs = 0
                except FailedJobsException as e:
                    numberOfFailedJobs = e.numberOfFailedJobs
                    if totalTrys > 32:  # p(fail after this many restarts) ~= 0.5**32
                        pytest.fail("Exceeded a reasonable number of restarts")
                    totalTrys += 1

            # For each job check it created a valid output file and add the ordering
            # relationships contained within the output file to the ordering relationship,
            # so we can check they are compatible with the relationships defined by the job DAG.
            ordering = None
            for i in range(nodeNumber):
                with (tempDir / str(i)).open() as fH:
                    ordering = list(map(int, fH.readline().split()))
                    assert int(ordering[-1]) == i
                    for j in ordering[:-1]:
                        adjacencyList[int(j)].add(i)
            # Check the ordering retains an acyclic graph
            if not self.isAcyclic(adjacencyList):
                print("ORDERING", ordering)
                print("CHILD EDGES", childEdges)
                print("FOLLOW ON EDGES", followOnEdges)
                print("ADJACENCY LIST", adjacencyList)
            assert self.isAcyclic(adjacencyList)

    @staticmethod
    def getRandomEdge(nodeNumber: int) -> tuple[int, int]:
        assert nodeNumber > 1
        fNode = random.choice(range(nodeNumber - 1))
        return fNode, random.choice(range(fNode + 1, nodeNumber))

    @staticmethod
    def makeRandomDAG(nodeNumber: int) -> set[tuple[int, int]]:
        """
        Makes a random dag with "nodeNumber" nodes in which all nodes are connected. Return value
        is list of edges, each of form (a, b), where a and b are integers >= 0 < nodeNumber
        referring to nodes and the edge is from a to b.
        """
        # Pick number of total edges to create
        edgeNumber = random.choice(
            range(nodeNumber - 1, 1 + (nodeNumber * (nodeNumber - 1) // 2))
        )
        # Make a spanning tree of edges so that nodes are connected
        edges = {(random.choice(range(i)), i) for i in range(1, nodeNumber)}
        # Add extra random edges until there are edgeNumber edges
        while len(edges) < edgeNumber:
            edges.add(TestJob.getRandomEdge(nodeNumber))
        return edges

    @staticmethod
    def getAdjacencyList(
        nodeNumber: int, edges: set[tuple[int, int]]
    ) -> list[set[int]]:
        """
        Make adjacency list representation of edges
        """
        adjacencyList: list[set[int]] = [set() for _ in range(nodeNumber)]
        for fNode, tNode in edges:
            adjacencyList[fNode].add(tNode)
        return adjacencyList

    def reachable(self, node: int, adjacencyList: list[set[int]]) -> set[int]:
        """
        Find the set of nodes reachable from this node (including the node). Return is a set of
        integers.
        """
        visited: set[int] = set()

        def dfs(fNode: int) -> None:
            if fNode not in visited:
                visited.add(fNode)
                list(map(dfs, adjacencyList[fNode]))

        dfs(node)
        return visited

    def addRandomFollowOnEdges(
        self, childAdjacencyList: list[set[int]]
    ) -> set[tuple[int, int]]:
        """
        Adds random follow on edges to the graph, represented as an adjacency list. The follow on
        edges are returned as a set and their augmented edges are added to the adjacency list.
        """

        def makeAugmentedAdjacencyList() -> list[set[int]]:
            augmentedAdjacencyList = [
                childAdjacencyList[i].union(followOnAdjacencyList[i])
                for i in range(len(childAdjacencyList))
            ]

            def addImpliedEdges(node: int, followOnEdges: set[int]) -> None:
                # Let node2 be a child of node or a successor of a child of node.
                # For all node2 the following adds an edge to the augmented
                # adjacency list from node2 to each followOn of node

                visited: set[int] = set()

                def f(node2: int) -> None:
                    if node2 not in visited:
                        visited.add(node2)
                        for i in followOnEdges:
                            augmentedAdjacencyList[node2].add(i)
                        list(map(f, childAdjacencyList[node2]))
                        list(map(f, followOnAdjacencyList[node2]))

                list(map(f, childAdjacencyList[node]))

            for node in range(len(followOnAdjacencyList)):
                addImpliedEdges(node, followOnAdjacencyList[node])
            return augmentedAdjacencyList

        followOnEdges = set()
        followOnAdjacencyList: list[set[int]] = [set() for i in childAdjacencyList]
        # Loop to create the follow on edges (try 1000 times)
        while random.random() > 0.001:
            fNode, tNode = TestJob.getRandomEdge(len(childAdjacencyList))

            # Make an adjacency list including augmented edges and proposed
            # follow on edge

            # Add the new follow on edge
            followOnAdjacencyList[fNode].add(tNode)

            augmentedAdjacencyList = makeAugmentedAdjacencyList()

            # If the augmented adjacency doesn't contain a cycle then add the follow on edge to
            # the list of follow ons else remove the follow on edge from the follow on adjacency
            # list.
            if self.isAcyclic(augmentedAdjacencyList):
                followOnEdges.add((fNode, tNode))
            else:
                followOnAdjacencyList[fNode].remove(tNode)

        # Update adjacency list adding in augmented edges
        childAdjacencyList[:] = makeAugmentedAdjacencyList()[:]

        return followOnEdges

    def makeJobGraph(
        self,
        nodeNumber: int,
        childEdges: set[tuple[int, int]],
        followOnEdges: set[tuple[int, int]],
        outPath: Path | None,
        addServices: bool = True,
    ) -> Job:
        """
        Converts a DAG into a job graph. childEdges and followOnEdges are the lists of child and
        followOn edges.
        """
        # Map of jobs to the list of promises they have
        jobsToPromisesMap: dict[FunctionWrappingJob, list[Promise]] = {}

        def makeJob(string: str) -> FunctionWrappingJob:
            promises: list[Promise] = []
            job = Job.wrapFn(
                fn2Test,
                promises,
                string,
                None if outPath is None else os.path.join(outPath, string),
                cores=0.1,
                memory="0.5G",
                disk="0.1G",
            )
            jobsToPromisesMap[job] = promises
            return job

        # Make the jobs
        jobs = [makeJob(str(i)) for i in range(nodeNumber)]

        # Record predecessors for sampling
        predecessors: dict[FunctionWrappingJob, list[FunctionWrappingJob]] = (
            collections.defaultdict(list)
        )

        # Make the edges
        for fNode, tNode in childEdges:
            jobs[fNode].addChild(jobs[tNode])
            predecessors[jobs[tNode]].append(jobs[fNode])
        for fNode, tNode in followOnEdges:
            jobs[fNode].addFollowOn(jobs[tNode])
            predecessors[jobs[tNode]].append(jobs[fNode])

        # Map of jobs to return values
        jobsToRvs = {
            job: (
                job.addService(
                    TrivialService(
                        cast(str, job.rv()), cores=0.1, memory="0.5G", disk="0.1G"
                    )
                )
                if addServices
                else job.rv()
            )
            for job in jobs
        }

        def getRandomPredecessor(job: FunctionWrappingJob) -> FunctionWrappingJob:
            predecessor = random.choice(list(predecessors[job]))
            while random.random() > 0.5 and len(predecessors[predecessor]) > 0:
                predecessor = random.choice(list(predecessors[predecessor]))
            return predecessor

        # Connect up set of random promises compatible with graph
        while random.random() > 0.01:
            job = random.choice(list(jobsToPromisesMap.keys()))
            promises = jobsToPromisesMap[job]
            if len(predecessors[job]) > 0:
                predecessor = getRandomPredecessor(job)
                promises.append(jobsToRvs[predecessor])

        return jobs[0]

    def isAcyclic(self, adjacencyList: list[set[int]]) -> bool:
        """
        Returns true if there are any cycles in the graph, which is represented as an adjacency
        list.
        """

        def cyclic(fNode: int, visited: set[int], stack: list[int]) -> bool | int:
            if fNode not in visited:
                visited.add(fNode)
                assert fNode not in stack
                stack.append(fNode)
                for tNode in adjacencyList[fNode]:
                    if cyclic(tNode, visited, stack):
                        return True
                assert stack.pop() == fNode
            return fNode in stack

        visited: set[int] = set()
        for i in range(len(adjacencyList)):
            if cyclic(i, visited, []):
                return False
        return True


def simpleJobFn(job: ServiceHostJob, value: str) -> None:
    job.fileStore.log_to_leader(value)


def fn1Test(string: str, outputFile: FileDescriptorOrPath) -> str:
    """
    Function appends the next character after the last character in the given
    string to the string, writes the string to a file, and returns it. For
    example, if string is "AB", we will write and return "ABC".
    """

    rV = string + chr(ord(string[-1]) + 1)
    with open(outputFile, "w") as fH:
        fH.write(rV)
    return rV


def fn2Test(pStrings: list[str], s: str, outputFile: Path) -> str:
    """
    Function concatenates the strings in pStrings and s, in that order, and writes the result to
    the output file. Returns s.
    """
    with open(outputFile, "w") as fH:
        fH.write(" ".join(pStrings) + " " + s)
    return s


def trivialParent(job: Job) -> None:
    strandedJob = JobFunctionWrappingJob(child)
    failingJob = JobFunctionWrappingJob(errorChild)

    job.addChild(failingJob)
    job.addChild(strandedJob)
    failingJob.addChild(strandedJob)


def parent(job: Job) -> None:
    childJob = JobFunctionWrappingJob(child)
    strandedJob = JobFunctionWrappingJob(child)
    failingJob = JobFunctionWrappingJob(errorChild)

    job.addChild(childJob)
    job.addChild(strandedJob)
    childJob.addChild(failingJob)
    failingJob.addChild(strandedJob)


def diamond(job: Job) -> None:
    childJob = JobFunctionWrappingJob(child)
    strandedJob = JobFunctionWrappingJob(child)
    failingJob = JobFunctionWrappingJob(errorChild)

    job.addChild(childJob)
    job.addChild(failingJob)
    childJob.addChild(strandedJob)
    failingJob.addChild(strandedJob)


def child(job: Job) -> None:
    assert job.cores is not None
    assert job.disk is not None
    assert job.memory is not None
    assert job.preemptible is not None


def errorChild(job: Job) -> NoReturn:
    raise RuntimeError("Child failure")


class TrivialService(Job.Service):
    def __init__(self, message: str, *args: Any, **kwargs: Any) -> None:
        """Service that does nothing, used to check for deadlocks"""
        Job.Service.__init__(self, *args, **kwargs)
        self.message = message

    def start(self, job: ServiceHostJob) -> str:
        return self.message

    def stop(self, job: ServiceHostJob) -> None:
        pass

    def check(self) -> bool:
        return True
