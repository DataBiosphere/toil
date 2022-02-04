#!/usr/bin/env cwl-runner
cwlVersion: v1.0
class: CommandLineTool
id: echo
inputs:
  - id: inp
    type: string
    default: hello
    inputBinding: {}
outputs: []
baseCommand: echo
stdout: out.txt