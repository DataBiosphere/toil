cwlVersion: v1.2
class: Workflow
doc: >
  The first step writes a File, and the second step copies it, but fails while
  a file exists at the path given as flag.

inputs:
  - id: flag
    type: string

steps:
  first:
    run:
      class: CommandLineTool
      inputs: []
      baseCommand: [sh, -c, 'echo first > first.txt']
      outputs:
        out:
          type: File
          outputBinding:
            glob: first.txt
    in: []
    out:
      - id: out

  second:
    run:
      class: CommandLineTool
      inputs:
        file: File
        flag: string
      baseCommand: [sh, -c, 'test ! -e "$1" && cat "$0" > second.txt']
      arguments: [ $(inputs.file.path), $(inputs.flag) ]
      outputs:
        out:
          type: File
          outputBinding:
            glob: second.txt
    in:
      - id: file
        source: first/out
      - id: flag
        source: flag
    out:
      - id: out

outputs:
  - id: out
    type: File
    outputSource: second/out
