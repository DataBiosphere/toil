# Copyright (C) 2015 UCSC Computational Genomics Lab
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
from __future__ import absolute_import
import unittest
import os
import random

from toil.lib.bioio import getTempFile
from toil.job import Job, JobGraphDeadlockException
from toil.test import ToilTest


class JobTest(ToilTest):
    """
    Tests testing the job class
    """
    def testStatic(self):
        """
        Create a DAG of jobs non-dynamically and run it. DAG is:
        
        A -> F
        \-------
        B -> D  \ 
         \       \
          ------- C -> E
          
        Follow on is marked by ->
        """
        outFile = getTempFile(rootDir=self._createTempDir())
        try:

            # Create the jobs
            A = Job.wrapFn(f, "A", outFile)
            B = Job.wrapFn(f, A.rv(), outFile)
            C = Job.wrapFn(f, B.rv(), outFile)
            D = Job.wrapFn(f, C.rv(), outFile)
            E = Job.wrapFn(f, D.rv(), outFile)
            F = Job.wrapFn(f, E.rv(), outFile)
            # Connect them into a workflow
            A.addChild(B)
            A.addChild(C)
            B.addChild(C)
            B.addFollowOn(E)
            C.addFollowOn(D)
            A.addFollowOn(F)

            # Create the runner for the workflow.
            options = Job.Runner.getDefaultOptions(self._getTestJobStorePath())
            options.logLevel = "INFO"
            # Run the workflow, the return value being the number of failed jobs
            Job.Runner.startToil(A, options)

            # Check output
            self.assertEquals(open(outFile, 'r').readline(), "ABCDEF")
        finally:
            os.remove(outFile)
    
    def testStatic2(self):
        """
        Create a DAG of jobs non-dynamically and run it. DAG is:
        
        A -> F
        \-------
        B -> D  \ 
         \       \
          ------- C -> E
          
        Follow on is marked by ->
        """
        outFile = getTempFile(rootDir=self._createTempDir())
        try:

            # Create the jobs
            A = Job.wrapFn(f, "A", outFile)
            B = Job.wrapFn(f, "B", outFile)
            C = Job.wrapFn(f, "C", outFile)
            D = Job.wrapFn(f, B.rv(), outFile)
            
            # Connect them into a workflow
            A.addChild(B)
            A.addFollowOn(C)
            C.addChild(D)

            # Create the runner for the workflow.
            options = Job.Runner.getDefaultOptions(self._getTestJobStorePath())
            options.logLevel = "INFO"
            # Run the workflow, the return value being the number of failed jobs
            Job.Runner.startToil(A, options)

            # Check output
            self.assertEquals(open(outFile, 'r').readline(), "ABCC")
        finally:
            os.remove(outFile)

    def testDeadlockDetection(self):
        """
        Randomly generate job graphs with various types of cycle in them and
        check they cause an exception properly. Also check that multiple roots 
        causes a deadlock exception.
        """
        for test in xrange(100):
            # Make a random DAG for the set of child edges
            nodeNumber = random.choice(xrange(2, 20))
            childEdges = self.makeRandomDAG(nodeNumber)
            # Get an adjacency list representation and check is acyclic
            adjacencyList = self.getAdjacencyList(nodeNumber, childEdges)
            self.assertTrue(self.isAcyclic(adjacencyList))
            # Add in follow on edges - these are returned as a list, and as a set
            # of augmented edges in the adjacency list
            followOnEdges = self.addRandomFollowOnEdges(adjacencyList)
            self.assertTrue(self.isAcyclic(adjacencyList))
            # Make the job graph
            rootJob = self.makeJobGraph(nodeNumber, childEdges, followOnEdges, None)
            rootJob.checkJobGraphAcylic()  # This should not throw an exception
            rootJob.checkJobGraphConnected()  # Nor this
            # Check root detection explicitly
            self.assertEquals(rootJob.getRootJobs(), set((rootJob,)))

            # Test making multiple roots
            childEdges2 = childEdges.copy()
            childEdges2.add((nodeNumber, 1))  # This creates an extra root at "nodeNumber"
            rootJob2 = self.makeJobGraph(nodeNumber + 1, childEdges2, followOnEdges, None)
            try:
                rootJob2.checkJobGraphConnected()
                self.assertTrue(False)  # Multiple roots were not detected
            except JobGraphDeadlockException:
                pass  # This is the expected behaviour

            def checkChildEdgeCycleDetection(fNode, tNode):
                childEdges.add((fNode, tNode))  # Create a cycle
                adjacencyList[fNode].add(tNode)
                self.assertTrue(not self.isAcyclic(adjacencyList))
                try:
                    self.makeJobGraph(nodeNumber, childEdges,
                                      followOnEdges, None).checkJobGraphAcylic()
                    self.assertTrue(False)  # A cycle was not detected
                except JobGraphDeadlockException:
                    pass  # This is the expected behaviour
                # Remove the edges
                childEdges.remove((fNode, tNode))
                adjacencyList[fNode].remove(tNode)
                # Check is now acyclic again
                self.makeJobGraph(nodeNumber, childEdges,
                                  followOnEdges, None).checkJobGraphAcylic()

            # Now try adding edges that create a cycle

            ##Try adding a child edge from a descendant to an ancestor
            fNode, tNode = self.getRandomEdge(nodeNumber)
            while fNode not in self.reachable(tNode, adjacencyList):
                fNode, tNode = self.getRandomEdge(nodeNumber)
            checkChildEdgeCycleDetection(fNode, tNode)

            ##Try adding a self child edge
            node = random.choice(xrange(nodeNumber))
            checkChildEdgeCycleDetection(node, node)

            def checkFollowOnEdgeCycleDetection(fNode, tNode):
                followOnEdges.add((fNode, tNode))  # Create a cycle
                try:
                    self.makeJobGraph(nodeNumber, childEdges,
                                      followOnEdges, None).checkJobGraphAcylic()
                    # self.assertTrue(False) #The cycle was not detected
                except JobGraphDeadlockException:
                    pass  # This is the expected behaviour
                # Remove the edges
                followOnEdges.remove((fNode, tNode))
                # Check is now acyclic again
                self.makeJobGraph(nodeNumber, childEdges,
                                  followOnEdges, None).checkJobGraphAcylic()

            ##Try adding a follow on edge from a descendant to an ancestor
            fNode, tNode = self.getRandomEdge(nodeNumber)
            while fNode not in self.reachable(tNode, adjacencyList):
                fNode, tNode = self.getRandomEdge(nodeNumber)
            checkFollowOnEdgeCycleDetection(fNode, tNode)

            ##Try adding a self follow on edge
            node = random.choice(xrange(nodeNumber))
            checkFollowOnEdgeCycleDetection(node, node)

            ##Try adding a follow on edge between two nodes with shared descendants
            fNode, tNode = self.getRandomEdge(nodeNumber)
            if (len(self.reachable(tNode, adjacencyList). \
                            intersection(self.reachable(fNode, adjacencyList))) > 0 and
                        (fNode, tNode) not in childEdges):
                checkFollowOnEdgeCycleDetection(fNode, tNode)

    def testEvaluatingRandomDAG(self):
        """
        Randomly generate test input then check that the ordering of the running
        respected the constraints.
        """
        jobStore = self._getTestJobStorePath()
        for test in xrange(30):
            # Temporary file
            outFile = getTempFile(rootDir=os.getcwd())
            # Make a random DAG for the set of child edges
            nodeNumber = random.choice(xrange(2, 20))
            childEdges = self.makeRandomDAG(nodeNumber)
            # Get an adjacency list representation and check is acyclic
            adjacencyList = self.getAdjacencyList(nodeNumber, childEdges)
            self.assertTrue(self.isAcyclic(adjacencyList))
            # Add in follow on edges - these are returned as a list, and as a set
            # of augmented edges in the adjacency list
            followOnEdges = self.addRandomFollowOnEdges(adjacencyList)
            self.assertTrue(self.isAcyclic(adjacencyList))
            # Make the job graph
            rootJob = self.makeJobGraph(nodeNumber, childEdges, followOnEdges, outFile)
            # Run the job  graph
            options = Job.Runner.getDefaultOptions("%s.%i" % (jobStore, test))
            Job.Runner.startToil(rootJob, options)
            # Get the ordering add the implied ordering to the graph
            with open(outFile, 'r') as fH:
                ordering = map(int, fH.readline().split())
            # Check all the jobs were run
            self.assertEquals(set(ordering), set(xrange(nodeNumber)))
            # Add the ordering to the graph
            for i in xrange(nodeNumber - 1):
                adjacencyList[ordering[i]].add(ordering[i + 1])
            # Check the ordering retains an acyclic graph
            if not self.isAcyclic(adjacencyList):
                print "ORDERING", ordering
                print "CHILD EDGES", childEdges
                print "FOLLOW ON EDGES", followOnEdges
                print "ADJACENCY LIST", adjacencyList
            self.assertTrue(self.isAcyclic(adjacencyList))
            # Cleanup
            os.remove(outFile)

    @staticmethod
    def getRandomEdge(nodeNumber):
        assert nodeNumber > 1
        fNode = random.choice(xrange(nodeNumber - 1))
        return (fNode, random.choice(xrange(fNode, nodeNumber)))

    @staticmethod
    def makeRandomDAG(nodeNumber):
        """
        Makes a random dag with "nodeNumber" nodes in which all nodes are 
        connected. Return value is list of edges, each of form (a, b), 
        where a and b are integers >= 0 < nodeNumber 
        referring to nodes and the edge is from a to b.
        """
        # Pick number of total edges to create
        edgeNumber = random.choice(xrange(nodeNumber - 1, 1 + (nodeNumber * (nodeNumber - 1)) / 2))
        # Make a spanning tree of edges so that nodes are connected
        edges = set(map(lambda i: (random.choice(xrange(i)), i), xrange(1, nodeNumber)))
        # Add extra random edges until there are edgeNumber edges
        while edgeNumber < len(edges):
            edges.add(JobTest.getRandomEdge(nodeNumber))
        return edges

    @staticmethod
    def getAdjacencyList(nodeNumber, edges):
        """
        Make adjacency list representation of edges
        """
        adjacencyList = [set() for i in xrange(nodeNumber)]
        for fNode, tNode in edges:
            adjacencyList[fNode].add(tNode)
        return adjacencyList

    @staticmethod
    def reachable(node, adjacencyList, followOnAdjacencyList=None):
        """
        Find the set of nodes reachable from this node (including the node). 
        Return is a set of integers. 
        """
        visited = set()

        def dfs(fNode):
            if fNode not in visited:
                visited.add(fNode)
                for tNode in adjacencyList[fNode]:
                    dfs(tNode)
                if followOnAdjacencyList != None:
                    for tNode in followOnAdjacencyList[fNode]:
                        dfs(tNode)

        dfs(node)
        return visited

    @staticmethod
    def addRandomFollowOnEdges(childAdjacencyList):
        """
        Adds random follow on edges to the graph, represented as an adjacency list.
        The follow on edges are returned as a set and their augmented edges
        are added to the adjacency list.
        """
        # This function makes the augmented adjacency list
        def makeAugmentedAdjacencyList():
            augmentedAdjacencyList = map(lambda i: childAdjacencyList[i].union(followOnAdjacencyList[i]),
                                         range(len(childAdjacencyList)))

            def addImpliedEdges(node, followOnEdges):
                visited = set()

                def f(node):
                    if node not in visited:
                        visited.add(node)
                        for i in followOnEdges:
                            augmentedAdjacencyList[node].add(i)
                        map(f, childAdjacencyList[node])
                        map(f, followOnAdjacencyList[node])

                map(f, childAdjacencyList[node])

            for node in xrange(len(followOnAdjacencyList)):
                addImpliedEdges(node, followOnAdjacencyList[node])
            return augmentedAdjacencyList

        followOnEdges = set()
        followOnAdjacencyList = map(lambda i: set(), childAdjacencyList)
        # Loop to create the follow on edges (try 1000 times)
        while random.random() > 0.001:
            fNode, tNode = JobTest.getRandomEdge(len(childAdjacencyList))
            # Get the  descendants of fNode not on a path of edges starting with a follow-on
            # edge from fNode
            fDescendants = reduce(lambda i, j: i.union(j), map(lambda c:
                                                               JobTest.reachable(c, childAdjacencyList,
                                                                                 followOnAdjacencyList),
                                                               childAdjacencyList[fNode]), set())
            fDescendants.add(fNode)

            # Make an adjacency list including augmented edges and proposed
            # follow on edge

            # Add the new follow on edge
            followOnAdjacencyList[fNode].add(tNode)

            augmentedAdjacencyList = makeAugmentedAdjacencyList()

            # If the augmented adjacency doesn't contain a cycle then add the follow on edge
            # to the list of follow ons else remove the follow on edge from the follow on
            # adjacency list
            if JobTest.isAcyclic(augmentedAdjacencyList):
                followOnEdges.add((fNode, tNode))
            else:
                followOnAdjacencyList[fNode].remove(tNode)

        # Update adjacency list adding in augmented edges
        childAdjacencyList[:] = makeAugmentedAdjacencyList()[:]

        return followOnEdges

    @staticmethod
    def makeJobGraph(nodeNumber, childEdges, followOnEdges, outFile):
        """
        Converts a DAG into a job graph. childEdges and followOnEdges are
        the lists of child and followOn edges.
        """
        #Map of jobs to the list of promises they have
        jobsToPromisesMap = {}
        #Function for making job
        def makeJob(string):
            promises = []
            job = Job.wrapFn(f, string, outFile, promises)
            jobsToPromisesMap[job] = promises
            return job
        #Make the jobs
        jobs = map(lambda i: makeJob(str(i) + " "), xrange(nodeNumber))
        #Make the edges
        for fNode, tNode in childEdges:
            jobs[fNode].addChild(jobs[tNode])
        for fNode, tNode in followOnEdges:
            jobs[fNode].addFollowOn(jobs[tNode])
        #Function to get a random predecessor for a job
        def getRandomPredecessor(job):
            predecessor = random.choice(list(job._directPredecessors))
            while random.random() > 0.5 and len(predecessor._directPredecessors) > 0:
                predecessor = random.choice(list(predecessor._directPredecessors))
            return predecessor
        #Connect up set of random promises compatible with graph
        while random.random() > 0.01:
            job = random.choice(jobsToPromisesMap.keys())
            promises = jobsToPromisesMap[job]
            if len(job._directPredecessors) > 0:
                predecessor = getRandomPredecessor(job)
                promises.append(predecessor.rv())
                
        return jobs[0]

    @staticmethod
    def isAcyclic(adjacencyList):
        """
        Returns true if there are any cycles in the graph, which is represented as an
        adjacency list. 
        """

        def cyclic(fNode, visited, stack):
            if fNode not in visited:
                visited.add(fNode)
                assert fNode not in stack
                stack.append(fNode)
                for tNode in adjacencyList[fNode]:
                    if cyclic(tNode, visited, stack):
                        return True
                assert stack.pop() == fNode
            return fNode in stack

        visited = set()
        for i in xrange(len(adjacencyList)):
            if cyclic(i, visited, []):
                return False
        return True


def f(string, outFile, promises=[]):
    """
    Function appends string to output file, then returns the 
    next ascii character of the first character in the string, e.g.
    if string is "AA" returns "B"
    """
    fH = open(outFile, 'a')
    fH.write(string)
    fH.close()
    return chr(ord(string[0]) + 1)

if __name__ == '__main__':
    unittest.main()
