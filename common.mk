SHELL=bash

ifndef TOIL_HOME
$(error Please run "source environment.sh" or "source environment-dev.sh" in the toil repo root directory before running make commands.)
endif
