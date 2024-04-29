cwlVersion: v1.2
class: Workflow
id: scatter-duplicate-outputs

inputs:
  - id: toScatterOver
    type: string[]
    default: ['this','is','an','array','of','more','than','two','things']

steps:
  echo:
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
        source: toScatterOver
    scatter:
     - s
    out:
      - id: out
      - id: err

outputs:
  - id: echo_out
    type: File[]
    outputSource: ["echo/out"]


requirements:
  - class: ScatterFeatureRequirement
