cwlVersion: v1.2
class: Workflow

inputs:
  - id: arrayS
    type: string[]
    default: ['hello','world']

steps:
  hello:
    run:
      class: CommandLineTool
      inputs:
        s: string
      baseCommand: [echo]
      arguments: [ $(inputs.s)]
      outputs:
        out: stdout
        err: stderr
    in:
      - id: s
        source: arrayS
    scatter:
     - s
    out:
      - id: out
      - id: err

  list:
    run:
      class: CommandLineTool
      inputs:
       file: File
      baseCommand: [ ls , -lh]
      arguments: [ $(inputs.file) ]
      outputs: []
    in:
      - id: file
        linkMerge: merge_flattened
        source:
          - hello/out
          - hello/err
    scatter:
      - file
    out: []

outputs: []

requirements:
  - class: ScatterFeatureRequirement
  - class: MultipleInputFeatureRequirement
