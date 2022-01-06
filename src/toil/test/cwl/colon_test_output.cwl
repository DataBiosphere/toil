#!/usr/bin/env cwl-runner
class: CommandLineTool
cwlVersion: v1.0
hints:
  DockerRequirement:
    dockerPull: docker.io/bash:4.4
inputs:
  input_file: File
  outdir_name: string

baseCommand: [ bash, -c ]
arguments:
 - |
   mkdir $(inputs.outdir_name);
   cp $(inputs.input_file.path) $(inputs.outdir_name)/;
outputs:
  result:
    type: Directory
    outputBinding:
      glob: $(inputs.outdir_name)
