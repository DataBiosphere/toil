SHELL=bash

ifndef TOIL_HOME
$(error Please run "source environment" or "source environment.dev" in the toil repo root directory before running make commands.)
endif
