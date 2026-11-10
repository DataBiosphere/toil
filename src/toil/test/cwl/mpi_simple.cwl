#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: CommandLineTool
$namespaces:
  cwltool: "http://commonwl.org/cwltool#"

doc: |
  Trivial MPI test that prints the process IDs of each of the parallel
  processes. Requires Python (but you have cwltool running, right?)
  and an MPI implementation.

baseCommand: python3
requirements:
  cwltool:MPIRequirement:
    processes: 2
arguments: [-c, 'import os; print(os.getpid())']
inputs: []
outputs:
  pids:
    type: stdout
