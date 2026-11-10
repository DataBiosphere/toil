$namespaces:
  toil: "http://toil.ucsc-cgl.org/cwl#"

class: CommandLineTool
doc: "Need an accelerator, saying what the job needs of it"
cwlVersion: v1.2

inputs:
  - id: qubits
    type: int

outputs:
  output:
    type: File
    outputBinding:
      glob: output.txt

requirements:
  InlineJavascriptRequirement: {}

hints:
  toil:AcceleratorRequirement:
    kind: qpu
    properties:
      backend: sim
      qubits: $(inputs.qubits)

baseCommand: ["echo", "hello"]
stdout: output.txt
