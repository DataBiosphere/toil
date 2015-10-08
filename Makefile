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
extras. For example, to install Toil in develop mode with Mesos, AWS, Azure and CWL support, run

make develop extras=[mesos,aws,azure,cwl]

The 'sdist' target creates a source distribution of Toil suitable for hot-deployment (not
implemented yet).

The 'clean' target undoes the effect of 'develop', 'docs', and 'sdist'.

The 'docs' target uses Sphinx to create HTML documentation in the docs/_build directory

The 'test' target runs Toil's unit tests. Set the 'tests' variable to run a particular test, e.g.

make test tests=src/toil/test/sort/sortTest.py::SortTest::testSort

The 'pypi' target publishes the current commit of Toil to PyPI after enforcing that the working
copy and the index are clean, and tagging it as an unstable .dev build.

The 'pypi_stable' target is like 'pypi' except that it doesn't tag the build as an unstable build.
IOW, it publishes a stable release.

endef

export help

green=\033[0;32m
normal=\033[0m
red=\033[0;31m

all: help

help:
	@echo "$$help"

clean: _develop _sdist _pypi _docs

extras=

# Inside a virtualenv, we can't use pip with --user (http://stackoverflow.com/questions/1871549).
#
__user=$(shell python -c 'import sys; print "" if hasattr(sys, "real_prefix") else "--user"')

check_user_base_on_path:
	@echo "$(green)Checking if Python's user-specific bin directory is on the PATH ...$(normal)"
	@test -z "$(__user)" || python -c 'import site,sys,os; \
	bin_dir = os.path.join( site.USER_BASE, "bin" ) ; \
	path_entries = map( os.path.realpath, os.environ["PATH"].split( os.path.pathsep ) ) ; \
	result = os.path.realpath( bin_dir ) in path_entries ; \
	print "$(green)OK$(normal)" if result else "$(red)Please add %s to your PATH$(normal)" % bin_dir ; \
	sys.exit(0 if result else 1)'

develop: check_user_base_on_path
	$(pip) install $(__user) -e .$(extras)

_develop:
	- $(pip) uninstall -y toil
	- rm -rf src/*.egg-info

sdist:
	$(python) setup.py sdist

_sdist:
	- rm -rf dist

tests=src

test:
	$(python) setup.py test --pytest-args "-vv $(tests) --assert plain"

check_clean_working_copy:
	@echo "$(green)Checking if your working copy is clean ...$(normal)"
	@git diff --exit-code > /dev/null \
		|| ( echo "$(red)Your working copy looks dirty.$(normal)" ; false )
	@git diff --cached --exit-code > /dev/null \
		|| ( echo "$(red)Your index looks dirty.$(normal)" ; false )
	@test -z "$$(git ls-files --other --exclude-standard --directory)" \
		|| ( echo "$(red)You have are untracked files:$(normal)" \
			; git ls-files --other --exclude-standard --directory \
			; false )

check_running_on_jenkins:
	@echo "$(green)Checking if running on Jenkins ...$(normal)"
	test -n "$$BUILD_NUMBER" \
		|| ( echo "$(red)This target should only be invoked on Jenkins.$(normal)" ; false )

pypi: check_clean_working_copy check_running_on_jenkins
	$(python) setup.py egg_info --tag-build=.dev$$BUILD_NUMBER sdist bdist_egg upload

pypi_stable: check_clean_working_copy check_running_on_jenkins
	$(python) setup.py egg_info sdist bdist_egg upload

_pypi:
	- rm -rf build/

docs:
	cd docs && make html

_docs:
	cd docs && make clean
