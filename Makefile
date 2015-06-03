.PHONY: all test clean

all :
	echo Nothing to be done.

clean :
	echo Nothing to be done.

tests=job,sort,stats,static,mesos,awsjobstore,filejobstore,singlemachinebatchsystem,mesosbatchsystem

test :
	PYTHONPATH=$${PWD}/src python -m jobTree.test.allTests --logDebug --testLength=SHORT --tests=$(tests)
