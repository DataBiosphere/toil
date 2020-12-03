*Run standard workflows on workflow execution platforms in a platform-agnostic way.*

## Executive Summary

The Workflow Execution Service (WES) API provides a standard way for users to submit workflow requests to workflow execution systems, and to monitor their execution. This API lets users run a single workflow (currently [**CWL**](https://www.commonwl.org/) or [**WDL**](http://www.openwdl.org/) formatted workflows, other types may be supported in the future) on multiple different platforms, clouds, and environments.

Key features of the API:

+ can request that a workflow be run
+ can pass parameters to that workflow (e.g. input files, cmdline arguments)
+ can get information about running workflows (e.g. status, errors, output file locations)
+ can cancel a running workflow
