binPath = ./bin
files:=jobTreeRestart jobTreeStatus jobTreeKill jobTreeStats multijob

.PHONY: all test clean scripts

all : scripts

scripts: $(foreach j,${files}, ${binPath}/$j)

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

test : scripts
	PYTHONPATH=.. python allTests.py --testLength=SHORT
