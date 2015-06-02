.PHONY: all test clean

all :
	echo Nothing to be done.

clean :
	echo Nothing to be done.

test :
	PYTHONPATH=src python -m jobTree.test.allTests --testLength=SHORT
