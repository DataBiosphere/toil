python=/usr/bin/env python2.7

.PHONY: all test clean

all :
	echo Nothing to be done.

clean :
	echo Nothing to be done.

# Override on the command line to run a particular test, e.g. tests=toil.test.src.jobTest.JobTest
tests=discover -s src -p "*Test.py"
testLength=SHORT
testLogLevel=INFO
test :
	PYTHONPATH=$${PWD}/src TOIL_TEST_ARGS="--logLevel=$(testLogLevel) --testLength=$(testLength)" $(python) -m unittest $(tests)
