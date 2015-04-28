binPath = ./bin
files:=jobTreeRestart jobTreeStatus jobTreeKill jobTreeStats multijob

.PHONY: all test clean

all : $(foreach j,${files}, ${binPath}/$j)

${binPath}/% : utils/%.py
	mkdir -p $(dir $@)
	cp $< $@.tmp
	mv $@.tmp $@
	chmod +x $@

${binPath}/multijob : batchSystems/multijob.py
	mkdir -p $(dir $@)
	cp $< $@.tmp
	mv $@.tmp $@
	chmod +x $@

clean :
	rm -rf ${binPath}/

test :
	#Running python allTests.py
	PYTHONPATH=.. PATH=../../bin:$$PATH python allTests.py --testLength=SHORT
