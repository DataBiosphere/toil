# Copyright (C) 2015 UCSC Computational Genomics Lab
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

python=/usr/bin/env python2.7
pip=/usr/bin/env pip2.7

define help

Supported targets: 'develop', 'docs', 'sdist', 'clean', 'test', 'pypi', or 'pypi_stable'.

The 'develop' target creates an editable install (aka develop mode). Set the 'extras' variable to
 ensure that develop mode installs support for extras. Consult setup.py for a list of supported
 extras. For example, to install Mesos and AWS support run

make develop extras=[mesos,aws]

The 'sdist' target creates a source distribution of Toil suitable for hot-deployment (not
implemented yet).

The 'clean' target undoes the effect of 'develop', 'docs', and 'sdist'.

The 'docs' target uses Sphinx to create html documentation in the doc/_build directory

The 'test' target runs Toil's unit tests. Set the 'tests' variable to run a particular test, e.g.

make test tests=toil.test.src.jobTest.JobTest

The 'pypi' target publishes the current commit of Toil to PyPI after enforcing that the working
copy and the index are clean, and tagging it as an unstable .dev build.

The 'pypi_stable' target is like 'pypi' except that it doesn't tag the build as an unstable build.
IOW, it publishes a stable release.

endef

export help

all:
	@echo "$$help"

clean: _develop _sdist _docs

extras=

# Inside a virtualenv, we can't use pip with --user (http://stackoverflow.com/questions/1871549).
#
__user=$(shell python -c 'import sys; print "" if hasattr(sys, "real_prefix") else "--user"')

check_user_base_on_path:
	@echo "\033[0;32mChecking if Python's user-specific bin directory is on the PATH ...\033[0m"
	@test -z "$(__user)" || python -c 'import site,sys,os;sys.exit(0 if os.path.join(site.USER_BASE,"bin") in os.environ["PATH"] else 1)'

develop: check_user_base_on_path
	$(pip) install $(__user) -e .$(extras)

_develop:
	- $(pip) uninstall -y toil
	- rm -rf src/*.egg-info

sdist:
	$(python) setup.py sdist

_sdist:
	- rm -rf dist

testLength=SHORT
testLogLevel=INFO

test:
	TOIL_TEST_ARGS="--logLevel=$(testLogLevel) --testLength=$(testLength)" \
	$(python) setup.py test --pytest-args "src -vv"

check_clean_working_copy:
	@echo "\033[0;32mChecking if your working copy is clean ...\033[0m"
	@git diff --exit-code > /dev/null || ( echo "\033[0;31mWorking copy looks dirty.\033[0m" ; false )
	git diff --cached --exit-code > /dev/null || ( echo "\033[0;31mIndex looks dirty.\033[0m" ; false )
	test -z "$(git ls-files --other --exclude-standard --directory)"

check_running_on_jenkins:
	@echo "\033[0;32mChecking if running on Jenkins ...\033[0m"
	@test -n "$$BUILD_NUMBER" || ( echo "\033[0;31mThis target should only be invoked on Jenkins.\033[0m" ; false )

pypi: check_clean_working_copy check_running_on_jenkins
	test "$$(git rev-parse --verify remotes/origin/master)" != "$$(git rev-parse --verify HEAD)" \
	&& echo "Not on master branch, silently skipping deployment to PyPI." \
	|| $(python) setup.py egg_info --tag-build=.dev$$BUILD_NUMBER register sdist bdist_egg upload

force_pypi: check_clean_working_copy check_running_on_jenkins
	$(python) setup.py egg_info --tag-build=.dev$$BUILD_NUMBER register sdist bdist_egg upload

pypi_stable: check_clean_working_copy check_running_on_jenkins
	test "$$(git rev-parse --verify remotes/origin/master)" != "$$(git rev-parse --verify HEAD)" \
	&& echo "Not on master branch, silently skipping deployment to PyPI." \
	|| $(python) setup.py egg_info register sdist bdist_egg upload

docs:
	cd docs && make html

_docs:
	cd docs && make clean
