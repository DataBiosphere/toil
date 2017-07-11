cwlVersion: v1.0

class: CommandLineTool
baseCommand: ["ls", "-1L"]
stdout: output.txt


inputs:
  dir:
    type: Directory
    inputBinding:
      position: 1

outputs:
  lsout:
    type: stdout
