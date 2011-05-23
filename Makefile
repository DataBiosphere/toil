binPath = ./bin
files:=jobTreeRun jobTreeStatus jobTreeKill jobTreeSlave jobTreeStats scriptTree multijob jobTreeTest_CommandFirst.py jobTreeTest_CommandSecond.py scriptTreeTest_Wrapper.py scriptTreeTest_Wrapper2.py scriptTreeTest_Sort.py

all : $(foreach j,${files}, ${binPath}/$j)

${binPath}/% : src/%.py
	mkdir -p $(dir $@)
	cp $< $@.tmp
	mv $@.tmp $@
	chmod +x $@

${binPath}/scriptTree : scriptTree/scriptTree.py
	mkdir -p $(dir $@)
	cp $< $@.tmp
	mv $@.tmp $@
	chmod +x $@

${binPath}/multijob : batchSystems/multijob.py
	mkdir -p $(dir $@)
	cp $< $@.tmp
	mv $@.tmp $@
	chmod +x $@

${binPath}/jobTreeTest_Command%.py : test/jobTree/jobTreeTest_Command%.py
	mkdir -p $(dir $@)
	cp $< $@.tmp
	mv $@.tmp $@
	chmod +x $@

${binPath}/scriptTreeTest_%.py : test/scriptTree/scriptTreeTest_%.py
	mkdir -p $(dir $@)
	cp $< $@.tmp
	mv $@.tmp $@
	chmod +x $@

${binPath}/scriptTreeTest_Sort.py : test/sort/scriptTreeTest_Sort.py
	mkdir -p $(dir $@)
	cp $< $@.tmp
	mv $@.tmp $@
	chmod +x $@

clean :
	rm -rf ${binPath}/

test :
#Running python allTests.py
	PYTHONPATH=.. PATH=../../bin:$$PATH python allTests.py --testLength=SHORT --logDebug
