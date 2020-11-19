# Copyright (C) 2015-2018 UCSC Computational Genomics Lab
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
include common.mk

define help

Supported targets: prepare, develop, docs, sdist, clean, test, docker, and push_docker.

Please note that all build targets require a virtualenv to be active.

The 'prepare' target installs Toil's build requirements into the current virtualenv.

The 'develop' target creates an editable install of Toil and its runtime requirements in the
current virtualenv. The install is called 'editable' because changes to the source code
immediately affect the virtualenv. Set the 'extras' variable to ensure that the 'develop' target
installs support for extras; some tests require extras to be installed. Consult setup.py for the
list of supported extras. To install Toil in develop mode with all extras, run

	make develop extras=[all]

The 'sdist' target creates a source distribution of Toil. It is used for some unit tests and for
installing the currently checked out version of Toil into the appliance image.

The 'clean' target cleans up the side effects of 'develop', 'sdist', 'docs', and 'docker'
on this machine. It does not undo externally visible effects like removing packages already
uploaded to PyPI.

The 'docs' target uses Sphinx to create HTML documentation in the docs/_build directory

Targets are provided to run Toil's tests. Note that these targets do *not* automatically install
Toil's dependencies; it is recommended to 'make develop' before running any of them.

The 'test' target runs Toil's unit tests serially with pytest. It will run some docker tests and
setup. Note: this target does not capture output from the terminal. For any of the test targets,
set the 'tests' variable to run a particular test, e.g.

	make test tests=src/toil/test/sort/sortTest.py::SortTest::testSort
    
The 'test_offline' target is similar, but runs only tests that don't need
Internet or Docker by default.  The 'test_offline' target takes the same tests
argument as the 'test' target.

    make test_offline tests=src/toil/test/src/threadingTest.py

The 'docker' target builds the Docker images that make up the Toil appliance. You may set the
TOIL_DOCKER_REGISTRY variable to override the default registry that the 'push_docker' target pushes
the appliance images to, for example:

	TOIL_DOCKER_REGISTRY=quay.io/USER make docker

If Docker is not installed, Docker-related targets tasks and tests will be skipped. The
same can be achieved by setting TOIL_DOCKER_REGISTRY to an empty string.

The 'push_docker' target pushes the Toil appliance images to a remote Docker registry. It
requires the TOIL_DOCKER_REGISTRY variable to be set to a value other than the default to avoid
accidentally pushing to the official Docker registry for Toil.

The TOIL_DOCKER_NAME environment variable can be set to customize the appliance image name that
is created by the 'docker' target and pushed by the 'push_docker' target.

endef
export help
help:
	@printf "$$help"


# This Makefile uses bash features like printf and <()
SHELL=bash
tests=src/toil/test
extras=
sdist_name:=toil-$(shell python version_template.py distVersion).tar.gz

green=\033[0;32m
normal=\033[0m
red=\033[0;31m
cyan=\033[0;36m

develop: check_venv
	pip install -e .$(extras)

clean_develop: check_venv
	- pip uninstall -y toil
	- rm -rf src/*.egg-info
	- rm src/toil/version.py

sdist: dist/$(sdist_name)

dist/$(sdist_name): check_venv
	@test -f dist/$(sdist_name) && mv dist/$(sdist_name) dist/$(sdist_name).old || true
	python setup.py sdist
	@test -f dist/$(sdist_name).old \
	    && ( cmp -s <(tar -xOzf dist/$(sdist_name)) <(tar -xOzf dist/$(sdist_name).old) \
	         && mv dist/$(sdist_name).old dist/$(sdist_name) \
	         && printf "$(cyan)No significant changes to sdist, reinstating backup.$(normal)\n" \
	         || rm dist/$(sdist_name).old ) \
	    || true

clean_sdist:
	- rm -rf dist
	- rm src/toil/version.py

# We always claim to be Travis, so that local test runs will not skip Travis tests.
test: check_venv check_build_reqs
	TRAVIS=true \
	    python -m pytest --cov=toil --duration=0 -s -r s $(tests)


# This target will skip building docker and all docker based tests
# these are our travis tests; rename?
test_offline: check_venv check_build_reqs
	@printf "$(cyan)All docker related tests will be skipped.$(normal)\n"
	TOIL_SKIP_DOCKER=True \
	TRAVIS=true \
	    python -m pytest -vv --timeout=530 --cov=toil $(tests)

ifdef TOIL_DOCKER_REGISTRY

docker_image:=$(TOIL_DOCKER_REGISTRY)/$(TOIL_DOCKER_NAME)

grafana_image:=$(TOIL_DOCKER_REGISTRY)/toil-grafana
prometheus_image:=$(TOIL_DOCKER_REGISTRY)/toil-prometheus
mtail_image:=$(TOIL_DOCKER_REGISTRY)/toil-mtail

define tag_docker
	@printf "$(cyan)Removing old tag $2. This may fail but that's expected.$(normal)\n"
	-docker rmi $2
	docker tag $1 $2
	@printf "$(green)Tagged appliance image $1 as $2.$(normal)\n"
endef

docker: docker/Dockerfile
	# Pre-pull everything
	for i in $$(seq 1 11); do if [[ $$i == "11" ]] ; then exit 1 ; fi ; docker pull ubuntu:16.04 && break || sleep 60; done
	for i in $$(seq 1 11); do if [[ $$i == "11" ]] ; then exit 1 ; fi ; docker pull prom/prometheus:v2.0.0 && break || sleep 60; done
	for i in $$(seq 1 11); do if [[ $$i == "11" ]] ; then exit 1 ; fi ; docker pull grafana/grafana && break || sleep 60; done
	for i in $$(seq 1 11); do if [[ $$i == "11" ]] ; then exit 1 ; fi ; docker pull sscaling/mtail && break || sleep 60; done

	@set -ex \
	; cd docker \
	; docker build --tag=$(docker_image):$(TOIL_DOCKER_TAG) -f Dockerfile .

	@set -ex \
	; cd dashboard/prometheus \
	; docker build --tag=$(prometheus_image):$(TOIL_DOCKER_TAG) -f Dockerfile .

	@set -ex \
	; cd dashboard/grafana \
	; docker build --tag=$(grafana_image):$(TOIL_DOCKER_TAG) -f Dockerfile .

	@set -ex \
	; cd dashboard/mtail \
	; docker build --tag=$(mtail_image):$(TOIL_DOCKER_TAG) -f Dockerfile .


docker/$(sdist_name): dist/$(sdist_name)
	cp $< $@

docker/Dockerfile: docker/Dockerfile.py docker/$(sdist_name)
	_TOIL_SDIST_NAME=$(sdist_name) python docker/Dockerfile.py > $@

clean_docker:
	-rm docker/Dockerfile docker/$(sdist_name)
	-docker rmi $(docker_image):$(TOIL_DOCKER_TAG)

push_docker: docker
	# Weird if logic is so we fail if all the pushes fail
	for i in $$(seq 1 6); do if [[ $$i == "6" ]] ; then exit 1 ; fi ; docker push $(docker_image):$(TOIL_DOCKER_TAG) && break || sleep 60; done
	for i in $$(seq 1 6); do if [[ $$i == "6" ]] ; then exit 1 ; fi ; docker push $(grafana_image):$(TOIL_DOCKER_TAG) && break || sleep 60; done
	for i in $$(seq 1 6); do if [[ $$i == "6" ]] ; then exit 1 ; fi ; docker push $(prometheus_image):$(TOIL_DOCKER_TAG) && break || sleep 60; done
	for i in $$(seq 1 6); do if [[ $$i == "6" ]] ; then exit 1 ; fi ; docker push $(mtail_image):$(TOIL_DOCKER_TAG) && break || sleep 60; done

else

docker docker_push clean_docker:
	@printf "$(cyan)Skipping '$@' target as TOIL_DOCKER_REGISTRY is empty or Docker is not installed.$(normal)\n"

endif


docs: check_venv check_build_reqs
	# Strange, but seemingly benign Sphinx warning floods stderr if not filtered:
	cd docs && make html

clean_docs: check_venv
	- cd docs && make clean

clean: clean_develop clean_sdist clean_docs

check_build_reqs:
	@(python -c 'import mock; import pytest' && which sphinx-build >/dev/null) \
		|| ( printf "$(red)Build requirements are missing. Run 'make prepare' to install them.$(normal)\n" ; false )

prepare: check_venv
	pip install mock==1.0.1 pytest==4.3.1 pytest-cov==2.6.1 stubserver==1.0.1 \
		pytest-timeout==1.3.3 setuptools==45.3.0 'sphinx>=2.4.4,<3' \
		cwltest mypy flake8 flake8-bugbear black isort pydocstyle

check_venv:
	@python -c 'import sys, os; sys.exit( int( 0 if "VIRTUAL_ENV" in os.environ else 1 ) )' \
		|| ( printf "$(red)A virtualenv must be active.$(normal)\n" ; false )

check_clean_working_copy:
	@printf "$(green)Checking if your working copy is clean ...$(normal)\n"
	@git diff --exit-code > /dev/null \
		|| ( printf "$(red)Your working copy looks dirty.$(normal)\n" ; false )
	@git diff --cached --exit-code > /dev/null \
		|| ( printf "$(red)Your index looks dirty.$(normal)\n" ; false )
	@test -z "$$(git ls-files --other --exclude-standard --directory)" \
		|| ( printf "$(red)You have untracked files:$(normal)\n" \
			; git ls-files --other --exclude-standard --directory \
			; false )

check_cpickle:
	# fail if cPickle.dump(s) called without HIGHEST_PROTOCOL
	# https://github.com/BD2KGenomics/toil/issues/1503
	! find src -iname '*.py' | xargs grep 'cPickle.dump' | grep --invert-match HIGHEST_PROTOCOL

format: $(wildcard src/toil/cwl/*.py)
	black $^

mypy:
	mypy --ignore-missing-imports --no-strict-optional \
		--warn-redundant-casts --warn-unused-ignores \
		$(CURDIR)/src/toil/cwl/cwltoil.py

.PHONY: help \
		prepare \
		check_cpickle \
		develop clean_develop \
		sdist clean_sdist \
		test test_offline \
		docs clean_docs \
		clean \
		check_venv \
		check_clean_working_copy \
		check_build_reqs \
		docker clean_docker push_docker
