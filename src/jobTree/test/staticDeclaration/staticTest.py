import unittest
import os
import random

from jobTree.lib.bioio import getTempFile
from jobTree.target import Target
from jobTree.test import JobTreeTest

class StaticTest(JobTreeTest):
    """
    Tests creating a jobTree inline.
    """

    def testStatic1(self):
        """
        Create a dag of targets non-dynamically and run it.
        
        A -> F
        \-------
        B -> D  \ 
         \       \
          ------- C -> E
          
        Follow on is marked by ->
        """
        #Temporary file
        outFile = getTempFile(rootDir=os.getcwd())
        
        #Create the targets
        A = Target.wrapFn(f, "A", outFile)
        B = Target.wrapFn(f, A.rv(0), outFile)
        C = Target.wrapFn(f, B.rv(0), outFile)
        D = Target.wrapFn(f, C.rv(0), outFile)
        E = Target.wrapFn(f, D.rv(0), outFile)
        F = Target.wrapFn(f, E.rv(0), outFile)
        
        #Connect them into a workflow
        A.addChild(B)
        A.addChild(C)
        B.addChild(C)
        B.addFollowOn(E)
        C.addFollowOn(D)
        A.addFollowOn(F)
        
        #Create the runner for the workflow.
        options = Target.Runner.getDefaultOptions()
        options.logLevel = "INFO"
        #Run the workflow, the return value being the number of failed jobs
        self.assertEquals(Target.Runner.startJobTree(A, options), 0)
        Target.Runner.cleanup(options) #This removes the jobStore
        
        #Check output
        self.assertEquals(open(outFile, 'r').readline(), "ABCDEF")
        
        #Cleanup
        os.remove(outFile)
        
    ##TODO Add tests for cycle detection
    
    def testCycleDetection(self):
        """
        Randomly generate target graphs with various types of cycle in them and
        check they cause an exception
        """
        for test in xrange(100): 
            #Make a random DAG for the set of child edges
            nodeNumber = random.choice(xrange(100))
            childEdges = self.makeRandomDAG(nodeNumber)
            #Get an adjacency list representation and check is acyclic 
            adjacencyList = self.getAdjacencyList(nodeNumber, childEdges)
            self.checkAcyclic(adjacencyList)
            #Add in follow on edges - these are returned as a list, and as a set
            #of augmented edges in the adjacency list
            followOnEdges = self.addRandomFollowOnEdges(adjacencyList)
            self.checkAcyclic(adjacencyList)
            #Make the target graph
            rootTarget = self.makeTargetGraph(nodeNumber, childEdges, followOnEdges, None)
            rootTarget.checkTargetGraphAcylic() #This should not throw an exception
            
            #Now try adding edges that create a cycle
            
            ##Try adding a child edge from a descendant to an ancestor
            
            ##Try adding a self child edge
            
            ##Try adding a follow on edge from a descendant to an ancestor
            
            ##Try adding a self follow on edge
            
            ##Try adding a follow on edge between two nodes with shared descendants
  
    def testEvaluatingRandomDAG(self):
        """
        Randomly generate test input then check it
        """
        for test in xrange(100): 
            #Temporary file
            outFile = getTempFile(rootDir=os.getcwd())
            #Make a random DAG for the set of child edges
            nodeNumber = random.choice(xrange(100))
            childEdges = self.makeRandomDAG(nodeNumber)
            #Get an adjacency list representation and check is acyclic 
            adjacencyList = self.getAdjacencyList(nodeNumber, childEdges)
            self.checkAcyclic(adjacencyList)
            #Add in follow on edges - these are returned as a list, and as a set
            #of augmented edges in the adjacency list
            followOnEdges = self.addRandomFollowOnEdges(adjacencyList)
            self.checkAcyclic(adjacencyList)
            #Make the target graph
            rootTarget = self.makeTargetGraph(nodeNumber, childEdges, followOnEdges, outFile)
            #Run the target  graph
            options = Target.Runner.getDefaultOptions()
            failedJobs = Target.Runner.startJobTree(rootTarget, options)
            self.assertEquals(failedJobs, 0)
            #Get the ordering add the implied ordering to the graph
            with open(outFile, 'r') as fH:
                ordering = fH.readline().split()
            #Check all the targets were run
            self.assertEquals(set(ordering), set(xrange(nodeNumber)))
            #Add the ordering to the graph
            for i in xrange(nodeNumber-1):
                adjacencyList[ordering[i]].add(ordering[i+1])
            #Check the ordering retains an acyclic graph
            self.checkAcyclic(adjacencyList)
            #Cleanup
            os.remove(outFile)
            
    @staticmethod
    def getRandomEdge(nodeNumber):
        fNode = random.choice(xrange(nodeNumber))
        return (fNode, random.choice(xrange(fNode+1,nodeNumber)))
    
    @staticmethod
    def makeRandomDAG(nodeNumber):
        """
        Makes a random dag with "nodeNumber" nodes in which all nodes are 
        connected. Return value is list of edges, each of form (a, b), 
        where a and b are integers >= 0 < nodeNumber 
        referring to nodes and the edge is from a to b.
        """
        #Pick number of total edges to create
        edgeNumber = random.choice(xrange((nodeNumber-1, nodeNumber * (nodeNumber-1)) / 2)) 
        #Make a spanning tree of edges so that nodes are connected
        edges = set(map(lambda i : (random.choice(xrange(i)), i), xrange(1, nodeNumber)))
        #Add extra random edges until there are edgeNumber edges
        while edgeNumber < len(edges):
            edges.add(StaticTest.getRandomEdge(nodeNumber))
        return edges
    
    @staticmethod
    def getAdjacencyList(nodeNumber, edges):
        """
        Make adjacency list representation of edges
        """
        adjacencyList = [ set() for i in xrange(nodeNumber) ]
        for fNode, tNode in edges:
            adjacencyList[fNode].add(tNode)
        return adjacencyList
    
    @staticmethod
    def addRandomFollowOnEdges(adjacencyList):
        """
        Adds random follow on edges to the graph, represented as an adjacency list.
        The follow on edges are returned as a set and their augmented edges
        are added to the adjacency list.
        """
        def reachable(node):
            visited = set()
            def dfs(fNode):
                if fNode not in visited:
                    visited.add(fNode)
                    for tNode in adjacencyList[fNode]:
                        dfs(tNode)
            dfs(node)
            return visited
            
        followOnEdges = set()
        #Loop to create the follow on edges
        while random.random() > 0.001:
            fNode, tNode = StaticTest.getRandomEdge(len(adjacencyList))
            fDescendants = reachable(fNode)
            tDescendants = reachable(tNode)
            #If there is no directed path from the fNode to the tNode can
            #create any subset of follow on edges between fNode and tDescendants
            if len(fDescendants.intersection(tDescendants)) == 0:
                for tNode2 in tDescendants:
                    if random.random() > 0.5:
                        followOnEdges.add((fNode, tNode2))
                        for descendant in fDescendants:
                            adjacencyList[descendant].add(tNode2)
                    
        return followOnEdges
    
    @staticmethod   
    def makeTargetGraph(nodeNumber, childEdges, followOnEdges, outFile):
        """
        Converts a DAG into a target graph. childEdges and followOnEdges are 
        the lists of child and followOn edges.
        """
        targets = map(lambda i : Target.wrapFn(f, str(i) + " ", outFile), xrange(nodeNumber))
        for fNode, tNode in childEdges:
            targets[fNode].addChild(targets[tNode])
        for fNode, tNode in followOnEdges:
            targets[fNode].addFollowOn(targets[tNode])
        return targets[0]
    
    def checkAcyclic(self, adjacencyList):
        """
        Checks if there are any cycles in the graph, which is represented as an
        adjacency list. If there are cycles, the test will fail
        """
        def dfs(fNode, visited, stack):
            if fNode not in visited:
                assert fNode not in stack
                stack.append(fNode)
                visited.add(fNode)
                for tNode in adjacencyList[fNode]:
                    dfs(tNode, visited, stack)
                assert stack.pop() == fNode
            self.assertTrue(fNode not in stack)
        visited = set()
        for i in len(adjacencyList):
            dfs(i, visited, [])

def f(string, outFile):
    """
    Function appends string to output file, then returns the 
    next ascii character of the first character in the string, e.g.
    if string is "AA" returns "B"
    """
    fH = open(outFile, 'a')
    fH.write(string)
    fH.close()   
    return chr(ord(string[0])+1)     

if __name__ == '__main__':
    unittest.main()