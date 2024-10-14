cwlVersion: v1.2
class: Workflow

inputs: []

steps:
  measure:
    run:
      class: CommandLineTool
      inputs: []
      baseCommand: ["bash", "-c", "ulimit -m"]
      outputs:
        memory: stdout
    in: []
    out:
      # There's no good way to go back from a command output to a CWL value
      # without bringing in a bunch of JS.
      - id: memory

outputs:
  - id: memory
    type: File
    outputSource: measure/memory
