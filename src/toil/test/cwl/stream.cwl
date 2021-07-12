# Example demonstrating files with flag 'streamable'=True are named pipes
class: Workflow
doc: "Stream a file"
cwlVersion: v1.2

inputs:
  input:
    type: File
    streamable: true

outputs:
  output:
    type: File
    outputSource: cat/output

steps:
  test:
    run:
      class: CommandLineTool
      baseCommand: ["test","-p"]
      arguments: [$(inputs.f)]
      outputs: []
      inputs:
        f: File
    out: []
    in:
      f:
        source: input
  cat:
    run:
      class: CommandLineTool
      baseCommand: ["cat"]
      arguments: [$(inputs.f)]
      outputs:
        output:
          type: stdout
      inputs:
        f: File
      stdout: output.txt
    out: [output]
    in:
      f:
        source: input
