.PHONY: all test clean

all :
	echo Nothing to be done.

clean :
	echo Nothing to be done.

test :
	PYTHONPATH=src python src/jobTree/test/allTests.py --testLength=SHORT
