cwlVersion: v1.0

class: CommandLineTool
baseCommand: ["mkdir", "-v", "-p"]

inputs:
  leaf:
    type: string
    inputBinding:
      position: 1

outputs:
  dirout:
    type: Directory
    outputBinding:
      glob: foo*
