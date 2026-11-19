cwlVersion: v1.2
class: CommandLineTool
doc: Write a file into an empty Directory literal given as an input, and return it.
requirements:
  InitialWorkDirRequirement:
    listing:
      - entry: $(inputs.target)
        writable: true
inputs:
  target: Directory
baseCommand: [touch]
arguments: [$(inputs.target.basename)/written]
outputs:
  filled:
    type: Directory
    outputBinding:
      glob: $(inputs.target.basename)
