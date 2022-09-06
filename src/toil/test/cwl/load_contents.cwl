class: CommandLineTool
doc: "Download a file from different cloud environments"
cwlVersion: v1.2
requirements:
  InlineJavascriptRequirement: {}

inputs:
  - id: input
    type: File
    inputBinding:
      position: 1
      loadContents: true

outputs:
  output:
    type: File
    outputBinding:
      glob: output.txt
  length:
    type: int
    outputBinding:
        outputEval: $(inputs.input.contents.length)


baseCommand: ["test","-f"]
stdout: output.txt
