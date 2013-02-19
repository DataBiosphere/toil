binPath = ./bin
files:=jobTreeRun jobTreeStatus jobTreeKill jobTreeStats multijob jobTreeTest_Dependencies.py scriptTreeTest_Wrapper.py scriptTreeTest_Wrapper2.py scriptTreeTest_Sort.py

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

${binPath}/jobTreeTest_%.py : test/jobTreeTest_%.py
	mkdir -p $(dir $@)
	cp $< $@.tmp
	mv $@.tmp $@
	chmod +x $@

${binPath}/scriptTreeTest_%.py : test/scriptTreeTest_%.py
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
	PYTHONPATH=.. PATH=../../bin:$$PATH python allTests.py --testLength=SHORT
