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

define help

Supported targets: 'develop', 'docs', 'sdist', 'clean', 'test', 'pypi', or 'pypi_stable'.

The 'develop' target creates an editable install (aka develop mode). Set the 'extras' variable to
ensure that develop mode installs support for extras. Consult setup.py for a list of supported
extras. To install Toil in develop mode with all extras, run

	make develop extras=[mesos,aws,azure,cwl,encryption]

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
help:
	@echo "$$help"


python=python2.7
pip=pip2.7
tests=src
extras=

green=\033[0;32m
normal=\033[0m
red=\033[0;31m


develop: check_venv
	$(pip) install -e .$(extras)
clean_develop: check_venv
	- $(pip) uninstall -y toil
	- rm -rf src/*.egg-info


sdist: check_venv
	$(python) setup.py sdist
clean_sdist:
	- rm -rf dist


test: check_venv
	$(python) setup.py test --pytest-args "-vv $(tests)"


pypi: check_venv check_clean_working_copy check_running_on_jenkins
	$(python) setup.py egg_info --tag-build=.dev$$BUILD_NUMBER sdist bdist_egg upload
pypi_stable: check_venv check_clean_working_copy check_running_on_jenkins
	$(python) setup.py egg_info sdist bdist_egg upload
clean_pypi:
	- rm -rf build/


docs: check_venv
	cd docs && make html
clean_docs: check_venv
	- cd docs && make clean


clean: clean_develop clean_sdist clean_pypi clean_docs


check_venv:
	@$(python) -c 'import sys; sys.exit( int( not hasattr(sys, "real_prefix") ) )' \
		|| ( echo "$(red)A virtualenv must be active.$(normal)" ; false )


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
	@test -n "$$BUILD_NUMBER" \
		|| ( echo "$(red)This target should only be invoked on Jenkins.$(normal)" ; false )


.PHONY: help develop clean_develop sdist clean_sdist test \
		pypi pypi_stable clean_pypi docs clean_docs clean \
		check_venv check_clean_working_copy check_running_on_jenkins
