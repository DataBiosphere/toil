cwlVersion: v1.0
class: CommandLineTool
baseCommand: sleep
stdout: output.txt
inputs:
  message:
    type: int
    inputBinding:
      position: 1
outputs:
  output:
    type: stdout
