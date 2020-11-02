cwlVersion: v1.0
class: CommandLineTool

requirements:
  - class: ShellCommandRequirement

baseCommand: []
stdout: output.txt

arguments:
  - position: 0
    shellQuote: false
    valueFrom: cat $(inputs.ref.secondaryFiles[0].path)

inputs:
  ref: { type: File, secondaryFiles: [.changedOnPorpoise] }

outputs:
  output:
    type: File
    outputBinding:
      glob: output.txt
