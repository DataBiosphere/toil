#!/usr/bin/env cwl-runner
# Command that will always fail
cwlVersion: v1.0
class: CommandLineTool
baseCommand: invalidcommand
inputs:
  message:
    type: string
    inputBinding:
      position: 1
outputs: []
