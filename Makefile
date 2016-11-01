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

Supported targets: prepare, develop, docs, sdist, clean, test, pypi, docker and push_docker.

Please note that all build targets require a virtualenv to be active. 

The 'prepare' target installs Toil's build requirements into the current virtualenv.

The 'develop' target creates an editable install of Toil and its runtime requirements in the 
current virtualenv. The install is called 'editable' because changes to the source code 
immediately affect the virtualenv. Set the 'extras' variable to ensure that the 'develop' target 
installs support for extras. Consult setup.py for the list of supported extras. To install Toil 
in develop mode with all extras, run 

	make develop extras=[mesos,aws,google,azure,cwl,encryption]

The 'sdist' target creates a source distribution of Toil suitable for hot-deployment (not
implemented yet).

The 'clean' target undoes the effect of 'develop', 'docs', and 'sdist'.

The 'docs' target uses Sphinx to create HTML documentation in the docs/_build directory

The 'test' target runs Toil's unit tests. Set the 'tests' variable to run a particular test, e.g.

	make test tests=src/toil/test/sort/sortTest.py::SortTest::testSort

The 'pypi' target publishes the current commit of Toil to PyPI after enforcing that the working
copy and the index are clean, and tagging it as an unstable .dev build.

The 'docker' target builds the Docker images that make up the Toil appliance.

The 'push_docker' target pushes the Toil appliance images to a remote Docker registry. It requires
the docker_registry variable to be set, e.g.

	make push_docker docker_registry=quay.io/USERNAME

endef
export help
help:
	@printf "$$help"

# This Makefile uses bash features like printf and <()
SHELL=bash
python=python2.7
pip=pip2.7
tests=src
extras=
toil_version:=$(shell $(python) version.py)
sdist_name:=toil-$(toil_version).tar.gz
current_commit:=$(shell git log --pretty=oneline -n 1 -- $(pwd) | cut -f1 -d " ")
dirty:=$(shell (git diff --exit-code && git diff --cached --exit-code) > /dev/null || printf -- --DIRTY)
docker_tag:=$(toil_version)--$(current_commit)$(dirty)
docker_base_name?=toil

green=\033[0;32m
normal=\033[0m\n
red=\033[0;31m


develop: check_venv
	$(pip) install -e .$(extras)
clean_develop: check_venv
	- $(pip) uninstall -y toil
	- rm -rf src/*.egg-info

sdist: dist/$(sdist_name)
dist/$(sdist_name): check_venv
	@test -f dist/$(sdist_name) && mv dist/$(sdist_name) dist/$(sdist_name).old || true
	$(python) setup.py sdist
	@test -f dist/$(sdist_name).old \
	    && ( cmp -s <(tar -xOzf dist/$(sdist_name)) <(tar -xOzf dist/$(sdist_name).old) \
	         && mv dist/$(sdist_name).old dist/$(sdist_name) \
	         && printf "$(green)No significant changes to sdist, reinstating backup.$(normal)" \
	         || rm dist/$(sdist_name).old ) \
	    || true
clean_sdist:
	- rm -rf dist


test: check_venv check_build_reqs docker
	TOIL_APPLIANCE_SELF=$(docker_registry)/$(docker_base_name):$(docker_tag) \
	    $(python) run_tests.py test $(tests)


integration-test: check_venv check_build_reqs sdist push_docker
	TOIL_TEST_INTEGRATIVE=True \
	TOIL_APPLIANCE_SELF=$(docker_registry)/$(docker_base_name):$(docker_tag) \
	    $(python) run_tests.py integration-test $(tests)


pypi: check_venv check_clean_working_copy check_running_on_jenkins
	set -x \
	&& tag_build=`$(python) -c 'pass;\
	    from version import version as v;\
	    from pkg_resources import parse_version as pv;\
	    import os;\
	    print "--tag-build=.dev" + os.getenv("BUILD_NUMBER") if pv(v).is_prerelease else ""'` \
	&& $(python) setup.py egg_info $$tag_build sdist bdist_egg upload
clean_pypi:
	- rm -rf build/


docker: check_docker_registry docker/Dockerfile
	@set -ex \
	; cd docker \
	; docker build --tag=$(docker_registry)/$(docker_base_name):$(docker_tag) \
	             -f Dockerfile \
	             .
	@printf "Tagged appliance image as $(docker_registry)/$(docker_base_name):$(docker_tag)\n"

docker/$(sdist_name): dist/$(sdist_name)
	cp $< $@

docker/Dockerfile: docker/Dockerfile.py docker/$(sdist_name)
	$(python) docker/Dockerfile.py \
	    --sdist=$(sdist_name) \
	    --self=$(docker_registry)/$(docker_base_name):$(docker_tag) > $@

clean_docker: check_docker_registry
	-rm docker/Dockerfile.{leader,worker} docker/$(sdist_name)
    -docker rmi $(docker_registry)/$(docker_base_name):$(docker_tag)

obliterate_docker: clean_docker
	-@set -x \
	; docker images $(docker_registry)/$(docker_base_name) \
	    | tail -n +2 | awk '{print $$1 ":" $$2}' | uniq \
	    | xargs docker rmi
	-docker images -qf dangling=true | xargs docker rmi

push_docker: docker
	docker push $(docker_registry)/$(docker_base_name):$(docker_tag)


docs: check_venv check_build_reqs
	# Strange, but seemingly benign Sphinx warning floods stderr if not filtered:
	cd docs && make html
clean_docs: check_venv
	- cd docs && make clean


clean: clean_develop clean_sdist clean_pypi clean_docs


check_build_reqs:
	@$(python) -c 'import mock; import pytest' \
		|| ( printf "$(red)Build requirements are missing. Run 'make prepare' to install them.$(normal)" ; false )


prepare: check_venv
	$(pip) install sphinx==1.4.1 mock==1.0.1 pytest==2.8.3 stubserver==1.0.1


check_venv:
	@$(python) -c 'import sys; sys.exit( int( not hasattr(sys, "real_prefix") ) )' \
		|| ( printf "$(red)A virtualenv must be active.$(normal)" ; false )


check_clean_working_copy:
	@printf "$(green)Checking if your working copy is clean ...$(normal)"
	@git diff --exit-code > /dev/null \
		|| ( printf "$(red)Your working copy looks dirty.$(normal)" ; false )
	@git diff --cached --exit-code > /dev/null \
		|| ( printf "$(red)Your index looks dirty.$(normal)" ; false )
	@test -z "$$(git ls-files --other --exclude-standard --directory)" \
		|| ( printf "$(red)You have are untracked files:$(normal)" \
			; git ls-files --other --exclude-standard --directory \
			; false )


check_running_on_jenkins:
	@printf "$(green)Checking if running on Jenkins ...$(normal)"
	@test -n "$$BUILD_NUMBER" \
		|| ( printf "$(red)This target should only be invoked on Jenkins.$(normal)" ; false )


check_docker_registry:
	@test -n "$(docker_registry)" \
		|| ( printf '$(red)Please set docker_registry, e.g. to quay.io/USER.$(normal)' ; false )


.PHONY: help \
		prepare \
		develop clean_develop \
		sdist clean_sdist \
		test \
		pypi clean_pypi \
		docs clean_docs \
		clean \
		check_venv \
		check_clean_working_copy \
		check_running_on_jenkins \
		check_build_reqs \
		docker clean_docker push_docker
