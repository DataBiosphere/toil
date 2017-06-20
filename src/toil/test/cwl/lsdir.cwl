cwlVersion: v1.0

class: CommandLineTool
baseCommand: ["ls", "-lL"]
stdout: output.txt


inputs:
  dir:
    type: Directory
    inputBinding:
      position: 1

outputs:
  duout:
    type: stdout
