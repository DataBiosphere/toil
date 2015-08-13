python=/usr/bin/env python2.7
pip=/usr/bin/env pip2.7

define help

Supported targets: 'develop', 'sdist', 'clean', 'test' or 'pypi'.

The 'develop' target creates an editable install (aka develop mode). Set the 'extras' variable to ensure that develop
mode installs support for extras. Consult setup.py for a list of supported extras. For example, to install Mesos and
AWS support run

make develop extras=[mesos,aws]

The 'sdist' target creates a source distribution of Toil suitable for hot-deployment (not implemented yet).

The 'clean' target undoes the effect of 'develop' and 'sdist'.

The 'test' target runs Toil's unit tests. Set the 'tests' variable to run a particular test, e.g.

make test tests=toil.test.src.jobTest.JobTest

The 'pypi' target deploys the current commit of Toil to PyPI after enforcing that the working copy and the index are
clean.

endef

export help

all:
	@echo "$$help"

clean: _develop _sdist

extras=

# Inside a virtualenv, we can't use pip with --user (http://stackoverflow.com/questions/1871549).
#
__user=$(shell python -c 'import sys; print "" if hasattr(sys, "real_prefix") else "--user"')

develop:
	$(pip) install $(__user) -e .$(extras)

_develop:
	- $(pip) uninstall -y toil
	- rm -rf src/*.egg-info

sdist:
	$(python) setup.py sdist

_sdist:
	- rm -rf dist

tests=discover -s src -p "*Test.py"
testLength=SHORT
testLogLevel=INFO

test:
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
