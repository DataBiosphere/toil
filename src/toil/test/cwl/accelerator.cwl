$namespaces:
  toil: "http://toil.ucsc-cgl.org/cwl#"

class: CommandLineTool
doc: "Need an accelerator that is not a GPU"
cwlVersion: v1.2

inputs:
  - id: count
    type: int
    default: 2

outputs:
  output:
    type: File
    outputBinding:
      glob: output.txt

requirements:
  InlineJavascriptRequirement: {}

hints:
  toil:AcceleratorRequirement:
    kind: fpga
    count: $(inputs.count)

baseCommand: ["echo", "hello"]
stdout: output.txt
