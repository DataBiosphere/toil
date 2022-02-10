#!/usr/bin/env cwl-runner
cwlVersion: v1.0
class: CommandLineTool
inputs:
  message:
    type: string
    default: hello
outputs:
  out:
    type: string
    outputBinding:
      glob: out.txt
      loadContents: true
      outputEval: $(self[0].contents)
baseCommand: bash
stderr: out.txt
arguments:
 - "-c"
 - "echo $(inputs.message) 1>&2"
