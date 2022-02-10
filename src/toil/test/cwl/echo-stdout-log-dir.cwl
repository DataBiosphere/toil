#!/usr/bin/env cwl-runner
cwlVersion: v1.0
class: CommandLineTool
id: echo
inputs:
  message:
    type: string
    default: hello
outputs: []
baseCommand: bash
arguments:
 - "-c"
 - "echo $(inputs.message) 1>&1"
stdout: out.txt
