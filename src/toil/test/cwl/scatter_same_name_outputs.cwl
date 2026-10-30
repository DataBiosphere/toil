cwlVersion: v1.2
class: Workflow
doc: >
  Each scattered job writes a File and a Directory with the same names as the
  other jobs' ones, and a later step reads all the Files.

inputs:
  - id: names
    type: string[]
    default: ['a', 'b', 'c']

steps:
  write:
    run:
      class: CommandLineTool
      inputs:
        name: string
      baseCommand: [sh, -c, 'echo "$0" > out.txt && mkdir res && echo "$0" > res/name.txt']
      arguments: [ $(inputs.name) ]
      outputs:
        out:
          type: File
          outputBinding:
            glob: out.txt
        res:
          type: Directory
          outputBinding:
            glob: res
    in:
      - id: name
        source: names
    scatter:
      - name
    out:
      - id: out
      - id: res

  gather:
    run:
      class: CommandLineTool
      inputs:
        files:
          type: File[]
          inputBinding: {}
      baseCommand: [cat]
      stdout: all.txt
      outputs:
        all: stdout
    in:
      - id: files
        source: write/out
    out:
      - id: all

outputs:
  - id: outs
    type: File[]
    outputSource: write/out
  - id: dirs
    type: Directory[]
    outputSource: write/res
  - id: all
    type: File
    outputSource: gather/all

requirements:
  - class: ScatterFeatureRequirement
