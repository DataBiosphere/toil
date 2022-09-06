class: CommandLineTool
doc: "Download a file from different cloud environments"
cwlVersion: v1.0


inputs:
  - id: input
    type: File
    inputBinding:
      position: 1

outputs:
  output:
    type: File
    outputBinding:
      glob: output.txt

baseCommand: ["test","-f"]
stdout: output.txt
