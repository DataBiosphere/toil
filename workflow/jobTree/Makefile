binPath = ../bin

all : ${binPath}/jobTree ${binPath}/jobTreeStatus ${binPath}/jobTreeKill ${binPath}/jobTreeSlave ${binPath}/jobTreeStats ${binPath}/scriptTree ${binPath}/jobTreeTest_CommandFirst.py ${binPath}/jobTreeTest_CommandSecond.py ${binPath}/scriptTreeTest_Wrapper.py ${binPath}/scriptTreeTest_Wrapper2.py ${binPath}/scriptTreeTest_Sort.py

${binPath}/jobTree : bin/jobTree.py
	cp bin/jobTree.py ${binPath}/jobTree
	chmod +x ${binPath}/jobTree

${binPath}/jobTreeStatus : bin/jobTreeStatus.py
	cp bin/jobTreeStatus.py ${binPath}/jobTreeStatus
	chmod +x ${binPath}/jobTreeStatus
	
${binPath}/jobTreeKill : bin/jobTreeKill.py
	cp bin/jobTreeKill.py ${binPath}/jobTreeKill
	chmod +x ${binPath}/jobTreeKill
	
${binPath}/jobTreeSlave : bin/jobTreeSlave.py
	cp bin/jobTreeSlave.py ${binPath}/jobTreeSlave
	chmod +x ${binPath}/jobTreeSlave
	
${binPath}/jobTreeStats : bin/jobTreeStats.py
	cp bin/jobTreeStats.py ${binPath}/jobTreeStats
	chmod +x ${binPath}/jobTreeStats
	
${binPath}/scriptTree : scriptTree/scriptTree.py
	cp scriptTree/scriptTree.py ${binPath}/scriptTree
	chmod +x ${binPath}/scriptTree
	
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
	rm -f ${binPath}/jobTree ${binPath}/jobTreeStatus ${binPath}/jobTreeKill ${binPath}/jobTreeSlave ${binPath}/jobTreeStats ${binPath}/scriptTree ${binPath}/jobTreeTest_CommandFirst.py ${binPath}/jobTreeTest_CommandSecond.py ${binPath}/scriptTreeTest_Wrapper.py ${binPath}/scriptTreeTest_Wrapper2.py ${binPath}/scriptTreeTest_Sort.py
