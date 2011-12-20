binPath = ./bin
files:=jobTreeRun jobTreeStatus jobTreeKill jobTreeSlave jobTreeStats multijob jobTreeTest_CommandFirst.py jobTreeTest_CommandSecond.py jobTreeTest_Dependencies.py scriptTreeTest_Wrapper.py scriptTreeTest_Wrapper2.py scriptTreeTest_Sort.py

.PHONY: all test clean

all : $(foreach j,${files}, ${binPath}/$j)

${binPath}/% : src/%.py
	mkdir -p $(dir $@)
	cp $< $@.tmp
	mv $@.tmp $@
	chmod +x $@

${binPath}/multijob : batchSystems/multijob.py
	mkdir -p $(dir $@)
	cp $< $@.tmp
	mv $@.tmp $@
	chmod +x $@

${binPath}/jobTreeTest_%.py : test/jobTree/jobTreeTest_%.py
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
