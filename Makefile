# Copyright (C) 2015-2021 Regents of the University of California
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

Supported targets: prepare, develop, docs, dist, clean, test, docker, and push_docker.

Please note that all build targets require a virtualenv to be active.

The 'prepare' target installs Toil's build requirements into the current virtualenv.

The 'develop' target creates an editable install of Toil and its runtime requirements in the
current virtualenv. The install is called 'editable' because changes to the source code
immediately affect the virtualenv. Set the 'extras' variable to ensure that the 'develop' target
installs support for extras; some tests require extras to be installed. Consult setup.py for the
list of supported extras. To install Toil in develop mode with all extras, run

	make develop extras=[all]

The 'dist' target creates a source distribution and a wheel of Toil. It is used
for some unit tests and for installing the currently checked out version of Toil
into the appliance image.

The 'clean' target cleans up the side effects of 'develop', 'dist', 'docs', and 'docker'
on this machine. It does not undo externally visible effects like removing packages already
uploaded to PyPI.

The 'docs' target uses Sphinx to create HTML documentation in the docs/_build directory

Targets are provided to run Toil's tests. Note that these targets do *not* automatically install
Toil's dependencies; it is recommended to 'make develop' before running any of them.

The 'test' target runs Toil's unit tests in parallel with pytest. It will run some docker tests and
setup. Note: this target does not capture output from the terminal. For any of the test targets,
set the 'tests' variable to run a particular test, e.g.

	make test tests=src/toil/test/sort/sortTest.py::SortTest::testSort
    
The 'cov' variable can be set to '' to suppress the test coverage report.
    
The 'test_offline' target is similar, but runs only tests that don't need
Internet or Docker by default.  The 'test_offline' target takes the same
arguments as the 'test' target.

    make test_offline tests=src/toil/test/src/threadingTest.py

The 'docker' target builds the Docker images that make up the Toil appliance. You may set the
TOIL_DOCKER_REGISTRY variable to override the default registry that the 'push_docker' target pushes
the appliance images to, for example:

	TOIL_DOCKER_REGISTRY=quay.io/USER make docker

You might also want to build just for one architecture and load into your
Docker daemon. We have a 'load_docker' target for this.

    make load_docker arch=amd64

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
arch=linux/amd64,linux/arm64
cov="--cov=toil"
extras=
# You can say make develop packages=xxx to install packages in the same Python
# environment as Toil itself without creating dependency conflicts with Toil
packages=
sdist_name:=toil-$(shell python3 version_template.py distVersion).tar.gz

green=\033[0;32m
normal=\033[0m
red=\033[0;31m
cyan=\033[0;36m

# expression to tell pytest what tests to run based on their marks
# the empty string means all tests will be run
marker=""
# Number of tests to run in parallel.
threads:="auto"

dist:="loadscope"
pytest_args:=""

# Only pass the threading options if running parallel tests. Otherwise we lose
# live logging. See <https://stackoverflow.com/q/62533239>
ifeq "$(threads)" "auto"
	threadopts=-n $(threads) --dist $(dist)
else
ifeq "$(threads)" "1"
	threadopts=
else
	threadopts=-n $(threads) --dist $(dist)
endif
endif

develop: check_venv
	pip install -e .$(extras) $(packages)

clean_develop: check_venv
	- rm -rf src/*.egg-info
	- rm src/toil/version.py

uninstall:
	- pip uninstall -y toil

dist: sdist
sdist: dist/$(sdist_name)

dist/$(sdist_name):
	@test -f dist/$(sdist_name) && mv dist/$(sdist_name) dist/$(sdist_name).old || true
	python3 -m build
	@test -f dist/$(sdist_name).old \
	    && ( cmp -s <(tar -xOzf dist/$(sdist_name)) <(tar -xOzf dist/$(sdist_name).old) \
	         && mv dist/$(sdist_name).old dist/$(sdist_name) \
	         && printf "$(cyan)No significant changes to sdist, reinstating backup.$(normal)\n" \
	         || rm dist/$(sdist_name).old ) \
	    || true

clean_sdist:
	- rm -rf dist
	- rm src/toil/version.py

download_cwl_spec:
	git clone https://github.com/common-workflow-language/cwl-v1.2.git src/toil/test/cwl/spec_v12 || true && cd src/toil/test/cwl/spec_v12 && git checkout 0d538a0dbc5518f3c6083ce4571926f65cb84f76
	git clone https://github.com/common-workflow-language/cwl-v1.1.git src/toil/test/cwl/spec_v11 || true && cd src/toil/test/cwl/spec_v11 && git checkout 664835e83eb5e57eee18a04ce7b05fb9d70d77b7
	git clone https://github.com/common-workflow-language/common-workflow-language.git src/toil/test/cwl/spec || true && cd src/toil/test/cwl/spec && git checkout 6a955874ade22080b8ef962b4e0d6e408112c1ef
        # Add .cwltest to filenames so the Pytest plugin can see them
	cp src/toil/test/cwl/spec_v12/conformance_tests.yaml src/toil/test/cwl/spec_v12/conformance_tests.cwltest.yaml
	cp src/toil/test/cwl/spec_v11/conformance_tests.yaml src/toil/test/cwl/spec_v11/conformance_tests.cwltest.yaml
	cp src/toil/test/cwl/spec/v1.0/conformance_test_v1.0.yaml src/toil/test/cwl/spec/v1.0/conformance_test_v1.0.cwltest.yaml


# Setting SET_OWNER_TAG will tag cloud resources so that UCSC's cloud murder bot won't kill them.
test: check_venv check_build_reqs
	TOIL_OWNER_TAG="shared" \
	    python -m pytest --log-format="%(asctime)s %(levelname)s %(message)s" --durations=0 --strict-markers --log-level DEBUG -o log_cli=true --log-cli-level INFO -r s $(cov) $(threadopts) $(tests) $(pytest_args) -m "$(marker)" --color=yes

test_debug: check_venv check_build_reqs
	TOIL_OWNER_TAG="$(whoami)" \
	    python -m pytest --log-format="%(asctime)s %(levelname)s %(message)s" --durations=0 --strict-markers --log-level DEBUG -s -o log_cli=true --log-cli-level DEBUG -r s $(tests) $(pytest_args) -m "$(marker)" --tb=native --maxfail=1 --color=yes


# This target will skip building docker and all docker based tests
# these are our travis tests; rename?
test_offline: check_venv check_build_reqs
	@printf "$(cyan)All docker related tests will be skipped.$(normal)\n"
	TOIL_SKIP_DOCKER=True \
	TOIL_SKIP_ONLINE=True \
	    python -m pytest --log-format="%(asctime)s %(levelname)s %(message)s" -vv --timeout=600 --strict-markers --log-level DEBUG --log-cli-level INFO $(cov) -n $(threads) --dist $(dist) $(tests) $(pytest_args) -m "$(marker)" --color=yes

# This target will run about 1 minute of tests, and stop at the first failure
test_1min: check_venv check_build_reqs
	TOIL_SKIP_DOCKER=True \
	    python -m pytest --log-format="%(asctime)s %(levelname)s %(message)s" -vv --timeout=10 --strict-markers --log-level DEBUG --log-cli-level INFO --maxfail=1 $(pytest_args) src/toil/test/batchSystems/batchSystemTest.py::SingleMachineBatchSystemTest::test_run_jobs src/toil/test/batchSystems/batchSystemTest.py::KubernetesBatchSystemBenchTest src/toil/test/server/serverTest.py::ToilWESServerBenchTest::test_get_service_info src/toil/test/cwl/cwlTest.py::CWLWorkflowTest::test_run_colon_output src/toil/test/jobStores/jobStoreTest.py::FileJobStoreTest::testUpdateBehavior -m "$(marker)" --color=yes

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

docker: toil_docker prometheus_docker grafana_docker mtail_docker

toil_docker: docker/Dockerfile
	mkdir -p .docker_cache
	@set -ex \
	; cd docker \
	; docker buildx build --platform=$(arch) --tag=$(docker_image):$(TOIL_DOCKER_TAG) --cache-from type=registry,ref=$(docker_image):$(TOIL_DOCKER_MAIN_CACHE_TAG) --cache-from type=registry,ref=$(docker_image):$(TOIL_DOCKER_CACHE_TAG) --cache-from type=local,src=../.docker-cache/toil --cache-to type=local,dest=../.docker-cache/toil -f Dockerfile .

prometheus_docker:
	mkdir -p .docker_cache
	@set -ex \
	; cd dashboard/prometheus \
	; docker buildx build --platform=$(arch) --tag=$(prometheus_image):$(TOIL_DOCKER_TAG) --cache-from type=registry,ref=$(prometheus_image):$(TOIL_DOCKER_MAIN_CACHE_TAG) --cache-from type=registry,ref=$(prometheus_image):$(TOIL_DOCKER_CACHE_TAG) --cache-from type=local,src=../../.docker-cache/prometheus --cache-to type=local,dest=../../.docker-cache/prometheus -f Dockerfile .

grafana_docker:
	mkdir -p .docker_cache
	@set -ex \
	; cd dashboard/grafana \
	; docker buildx build --platform=$(arch) --tag=$(grafana_image):$(TOIL_DOCKER_TAG) --cache-from type=registry,ref=$(grafana_image):$(TOIL_DOCKER_MAIN_CACHE_TAG) --cache-from type=registry,ref=$(grafana_image):$(TOIL_DOCKER_CACHE_TAG) --cache-from type=local,src=../../.docker-cache/grafana --cache-to type=local,dest=../../.docker-cache/grafana -f Dockerfile .

mtail_docker:
	mkdir -p .docker_cache
	@set -ex \
	; cd dashboard/mtail \
	; docker buildx build --platform=$(arch) --tag=$(mtail_image):$(TOIL_DOCKER_TAG) --cache-from type=registry,ref=$(mtail_image):$(TOIL_DOCKER_MAIN_CACHE_TAG) --cache-from type=registry,ref=$(mtail_image):$(TOIL_DOCKER_CACHE_TAG) --cache-from type=local,src=../../.docker-cache/mtail --cache-to type=local,dest=../../.docker-cache/mtail -f Dockerfile .

docker/$(sdist_name): dist/$(sdist_name)
	cp $< $@

docker/Dockerfile: docker/Dockerfile.py docker/$(sdist_name)
	_TOIL_SDIST_NAME=$(sdist_name) python docker/Dockerfile.py > $@

clean_docker:
	-rm docker/Dockerfile docker/$(sdist_name)
	-docker rmi $(docker_image):$(TOIL_DOCKER_TAG)

push_docker: docker
	# Weird if logic is so we fail if all the pushes fail.
	# We need to build from the local cache to the cache tag and again from the local cache to the real tag.
	cd docker ; \
	for i in $$(seq 1 6); do \
		if [[ $$i == "6" ]] ; then exit 1 ; fi ; \
		docker buildx build --platform $(arch) --push --tag=$(docker_image):$(TOIL_DOCKER_CACHE_TAG) --cache-from type=local,src=../.docker-cache/toil --cache-to type=inline -f Dockerfile . && \
			docker buildx build --platform $(arch) --push --tag=$(docker_image):$(TOIL_DOCKER_TAG) --cache-from type=local,src=../.docker-cache/toil -f Dockerfile . && \
			break || sleep 60; \
	done
	cd dashboard/prometheus ; \
	for i in $$(seq 1 6); do \
		if [[ $$i == "6" ]] ; then exit 1 ; fi ; \
		docker buildx build --platform $(arch) --push --tag=$(prometheus_image):$(TOIL_DOCKER_CACHE_TAG) --cache-from type=local,src=../../.docker-cache/prometheus --cache-to type=inline -f Dockerfile . && \
			docker buildx build --platform $(arch) --push --tag=$(prometheus_image):$(TOIL_DOCKER_TAG) --cache-from type=local,src=../../.docker-cache/prometheus -f Dockerfile . && \
			break || sleep 60; \
	done
	cd dashboard/grafana ; \
	for i in $$(seq 1 6); do \
		if [[ $$i == "6" ]] ; then exit 1 ; fi ; \
		docker buildx build --platform $(arch) --push --tag=$(grafana_image):$(TOIL_DOCKER_CACHE_TAG) --cache-from type=local,src=../../.docker-cache/grafana --cache-to type=inline -f Dockerfile . && \
			docker buildx build --platform $(arch) --push --tag=$(grafana_image):$(TOIL_DOCKER_TAG) --cache-from type=local,src=../../.docker-cache/grafana -f Dockerfile . && \
			break || sleep 60; \
	done
	cd dashboard/mtail ; \
	for i in $$(seq 1 6); do \
		if [[ $$i == "6" ]] ; then exit 1 ; fi ; \
		docker buildx build --platform $(arch) --push --tag=$(mtail_image):$(TOIL_DOCKER_CACHE_TAG) --cache-from type=local,src=../../.docker-cache/mtail --cache-to type=inline -f Dockerfile . && \
			docker buildx build --platform $(arch) --push --tag=$(mtail_image):$(TOIL_DOCKER_TAG) --cache-from type=local,src=../../.docker-cache/mtail -f Dockerfile . && \
			break || sleep 60; \
	done

load_docker: docker
	cd docker ; docker buildx build --platform $(arch) --load --tag=$(docker_image):$(TOIL_DOCKER_TAG) --cache-from type=local,src=../.docker-cache/toil -f Dockerfile .
	cd dashboard/prometheus ; docker buildx build --platform $(arch) --load --tag=$(prometheus_image):$(TOIL_DOCKER_TAG) --cache-from type=local,src=../../.docker-cache/prometheus -f Dockerfile .
	cd dashboard/grafana ; docker buildx build --platform $(arch) --load --tag=$(grafana_image):$(TOIL_DOCKER_TAG) --cache-from type=local,src=../../.docker-cache/grafana -f Dockerfile .
	cd dashboard/mtail ; docker buildx build --platform $(arch) --load --tag=$(mtail_image):$(TOIL_DOCKER_TAG) --cache-from type=local,src=../../.docker-cache/mtail -f Dockerfile .

else

docker push_docker load_docker clean_docker:
	@printf "$(cyan)Skipping '$@' target as TOIL_DOCKER_REGISTRY is empty or Docker is not installed.$(normal)\n"

endif


docs: check_venv check_build_reqs
	cd docs && ${MAKE} html

clean_docs: check_venv
	- cd docs && ${MAKE} clean

clean: clean_develop clean_sdist clean_docs

check_build_reqs:
	@(python -c 'import pytest' && which sphinx-build >/dev/null) \
		|| ( printf "$(red)Build requirements are missing. Run 'make prepare' to install them.$(normal)\n" ; false )

prepare: check_venv
	pip install -r requirements-dev.txt

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

PYSOURCES=$(shell find src -name '*.py') setup.py version_template.py

# Linting and code style related targets
## sorting imports using isort: https://github.com/timothycrosley/isort
sort_imports: $(PYSOURCES)
	isort -m VERTICAL $^ contrib/mypy-stubs
	${MAKE} format

remove_unused_imports: $(PYSOURCES)
	autoflake --in-place --remove-all-unused-imports $^
	${MAKE} format

remove_trailing_whitespace:
	$(CURDIR)/contrib/admin/remove_trailing_whitespace.py

format: $(PYSOURCES)
	black $^ contrib/mypy-stubs

mypy:
	MYPYPATH=$(CURDIR)/contrib/mypy-stubs mypy --strict $(CURDIR)/src/toil/{cwl/cwltoil.py,test/cwl/cwlTest.py}
	$(CURDIR)/contrib/admin/mypy-with-ignore.py
	
# This target will check any modified files for pylint errors.
# We have a lot of pylint errors already, because pylint can't understand our
# all our duck typing, but new ones can suggest that code won't actually work.
# Assumes an "upstream" remote
touched_pylint:
	pylint -E $(shell git diff --name-only upstream/master src | grep .py$$) || true

pydocstyle: src/toil
	pydocstyle --add-ignore=D100,D101,D102,D103,D104,D105,D107 setup.py $^ || true

pydocstyle_report.txt: src/toil
	pydocstyle setup.py $^ > $@ 2>&1 || true

diff_pydocstyle_report: pydocstyle_report.txt
	diff-quality --compare-branch=master --violations=pycodestyle --fail-under=100 $^

diff_mypy:
	mypy --cobertura-xml-report . src/toil > /dev/null || true
	diff-cover --fail-under=100 --compare-branch origin/master cobertura.xml

pyupgrade: $(PYSOURCES)
	pyupgrade --exit-zero-even-if-changed --py39-plus $^

flake8: $(PYSOURCES)
	flake8 --ignore=E501,W293,W291,E265,E302,E722,E126,E303,E261,E201,E202,W503,W504,W391,E128,E301,E127,E502,E129,E262,E111,E117,E306,E203,E231,E226,E741,E122,E251,E305,E701,E222,E225,E241,E305,E123,E121,E703,E704,E125,E402 $^

preflight: mypy touched_pylint

.PHONY: help \
		prepare \
		check_cpickle \
		develop clean_develop \
		sdist clean_sdist \
		download_cwl_spec \
		test test_offline test_1min \
		docs clean_docs \
		clean \
		sort_imports remove_unused_imports remove_trailing_whitespace \
		format mypy touched_pylint diff_pydocstyle_report diff_mypy pyupgrade flake8 preflight \
		check_venv \
		check_clean_working_copy \
		check_build_reqs \
		docker clean_docker push_docker \
		pre_pull_docker toil_docker prometheus_docker grafana_docker mtail_docker
