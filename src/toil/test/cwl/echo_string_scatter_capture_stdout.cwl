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
      stdout: "list.out"
      outputs: 
        list_out: stdout
    in:
      - id: file
        linkMerge: merge_flattened
        source:
          - hello/out
    scatter:
      - file
    out: 
      - id: list_out

outputs:
  - id: list_out
    type: File[]
    outputSource: ["list/list_out"]


requirements:
  - class: ScatterFeatureRequirement
  - class: MultipleInputFeatureRequirement
