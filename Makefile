python=/usr/bin/env python2.7
pip=/usr/bin/env pip2.7

all: sdist develop

clean: _develop _sdist

# Override this on the command line if you don't want to use any of these extras. Note that doing
# so will currently cause some unit tests to fail because they don't intelligently detect which
# extras were requested.
#
extras=[aws,mesos]

# Inside a virtualenv, we can't use pip with --user (http://stackoverflow.com/questions/1871549).
#
__user=$(shell python -c 'import sys; print "" if hasattr(sys, "real_prefix") else "--user"')

develop:
	$(pip) install $(__user) -e .$(extras)

_develop:
	- $(pip) uninstall -y toil

sdist:
	$(python) setup.py sdist

_sdist:
	- rm -rf dist

# Override this on the command line to run a particular test, e.g.
#
# make test tests=toil.test.src.jobTest.JobTest
#
tests=discover -s src -p "*Test.py"
testLength=SHORT
testLogLevel=INFO

test: develop
	TOIL_TEST_ARGS="--logLevel=$(testLogLevel) --testLength=$(testLength)" $(python) -m unittest $(tests)

# Override this to the empty string if you want to deploy a potentially dirty working copy to PyPI
#
pypi_prereqs=pypi_prereqs

pypi: $(pypi_prereqs)
	$(python) setup.py register sdist bdist_egg upload
	@echo "\033[0;32mLooks like the upload to PyPI was successful.\033[0m"
	@echo "\033[0;32mNow tag this release in git and bump the version in every setup.py.\033[0m"

pypi_prereqs:
	@echo "\033[0;32mChecking if your working copy is clean ...\033[0m"
	git diff --exit-code > /dev/null
	git diff --cached --exit-code > /dev/null
	test -z "$(git ls-files --other --exclude-standard --directory)"
	@echo "\033[0;32mLooks like your working copy is clean. Uploading to PyPI ...\033[0m"
