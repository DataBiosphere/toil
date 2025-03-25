#!/usr/bin/env cwl-runner
class: Workflow
cwlVersion: v1.2

inputs:
    inputFile:
      type: File
      secondaryFiles:
        - ^_old.zip?

outputs:
  out_file:
    type: File
    outputSource:
      - inputFile

steps:
  []
