$namespaces:
  toil: "http://toil.ucsc-cgl.org/cwl#"

class: CommandLineTool
doc: "Need an accelerator without saying what kind"
cwlVersion: v1.2

inputs: []

outputs:
  output:
    type: File
    outputBinding:
      glob: output.txt

hints:
  toil:AcceleratorRequirement:
    count: 1

baseCommand: ["echo", "hello"]
stdout: output.txt
