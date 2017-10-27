cwlVersion: v1.0
class: CommandLineTool
id: "seqtk_seq"
doc: "Convert to FASTA (seqtk)"
inputs:
  - id: input1
    type: File
    inputBinding:
      position: 1
      prefix: "-a"
outputs:
  - id: output1
    type: File
    outputBinding:
      glob: out
baseCommand: ["seqtk", "seq"]
arguments: []
stdout: out
hints:
  SoftwareRequirement:
    packages:
    - package: seqtk
      version:
      - r93
