cwlVersion: v1.2
class: Workflow
id: echo-test-scatter

inputs:
  - id: arrayS
    type: string[]
    default: ['hello','world']

steps:
  hello:
    run:
      id: scatter
      class: CommandLineTool
      inputs:
        s: string
      baseCommand: [echo]
      arguments: [ $(inputs.s)]
      stdout: scatter.out
      stderr: scatter.err
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
      id: list
      inputs:
       file: File
      baseCommand: [ ls , -lh]
      arguments: [ $(inputs.file) ]
      stdout: "list.out"
      stderr: "list.err"
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
