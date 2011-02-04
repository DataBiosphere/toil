binPath = ./bin

all : ${binPath}/jobTreeRun ${binPath}/jobTreeStatus ${binPath}/jobTreeKill ${binPath}/jobTreeSlave ${binPath}/jobTreeStats ${binPath}/scriptTree  ${binPath}/multijob ${binPath}/jobTreeTest_CommandFirst.py ${binPath}/jobTreeTest_CommandSecond.py ${binPath}/scriptTreeTest_Wrapper.py ${binPath}/scriptTreeTest_Wrapper2.py ${binPath}/scriptTreeTest_Sort.py

${binPath}/jobTreeRun : lib/jobTreeRun.py
	cp lib/jobTreeRun.py ${binPath}/jobTreeRun
	chmod +x ${binPath}/jobTreeRun

${binPath}/jobTreeStatus : lib/jobTreeStatus.py
	cp lib/jobTreeStatus.py ${binPath}/jobTreeStatus
	chmod +x ${binPath}/jobTreeStatus
	
${binPath}/jobTreeKill : lib/jobTreeKill.py
	cp lib/jobTreeKill.py ${binPath}/jobTreeKill
	chmod +x ${binPath}/jobTreeKill
	
${binPath}/jobTreeSlave : lib/jobTreeSlave.py
	cp lib/jobTreeSlave.py ${binPath}/jobTreeSlave
	chmod +x ${binPath}/jobTreeSlave
	
${binPath}/jobTreeStats : lib/jobTreeStats.py
	cp lib/jobTreeStats.py ${binPath}/jobTreeStats
	chmod +x ${binPath}/jobTreeStats
	
${binPath}/scriptTree : scriptTree/scriptTree.py
	cp scriptTree/scriptTree.py ${binPath}/scriptTree
	chmod +x ${binPath}/scriptTree
	
${binPath}/multijob : batchSystems/multijob.py
	cp batchSystems/multijob.py ${binPath}/multijob
	chmod +x ${binPath}/multijob
	
${binPath}/jobTreeTest_CommandFirst.py : test/jobTree/jobTreeTest_CommandFirst.py
	cp test/jobTree/jobTreeTest_CommandFirst.py ${binPath}/jobTreeTest_CommandFirst.py
	chmod +x ${binPath}/jobTreeTest_CommandFirst.py

${binPath}/jobTreeTest_CommandSecond.py : test/jobTree/jobTreeTest_CommandSecond.py
	cp test/jobTree/jobTreeTest_CommandSecond.py ${binPath}/jobTreeTest_CommandSecond.py
	chmod +x ${binPath}/jobTreeTest_CommandSecond.py
	
${binPath}/scriptTreeTest_Wrapper.py : test/scriptTree/scriptTreeTest_Wrapper.py
	cp test/scriptTree/scriptTreeTest_Wrapper.py ${binPath}/scriptTreeTest_Wrapper.py
	chmod +x ${binPath}/scriptTreeTest_Wrapper.py
	
${binPath}/scriptTreeTest_Wrapper2.py : test/scriptTree/scriptTreeTest_Wrapper2.py
	cp test/scriptTree/scriptTreeTest_Wrapper2.py ${binPath}/scriptTreeTest_Wrapper2.py
	chmod +x ${binPath}/scriptTreeTest_Wrapper2.py
	
${binPath}/scriptTreeTest_Sort.py : test/sort/scriptTreeTest_Sort.py
	cp test/sort/scriptTreeTest_Sort.py ${binPath}/scriptTreeTest_Sort.py
	chmod +x ${binPath}/scriptTreeTest_Sort.py

clean :
	rm -f ${binPath}/* doc/pydoc/*

tests :
	#Running python allTests.py
	PYTHONPATH=.. PATH=../../bin:$$PATH python allTests.py --testLength=SHORT --logDebug