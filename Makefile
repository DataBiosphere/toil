python=/usr/bin/env python2.7

.PHONY: all test clean

all :
	echo Nothing to be done.

clean :
	echo Nothing to be done.

# Override on the command line to run a particular test, e.g. tests=jobTree.test.src.jobTest.JobTest
tests=discover -s src -p "*Test.py"

test :
	PYTHONPATH=$${PWD}/src JOBTREE_TEST_ARGS="--logDebug --testLength=SHORT" $(python) -m unittest $(tests)
