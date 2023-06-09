cwlVersion: v1.2
class: CommandLineTool
baseCommand:
  - ls

requirements:
  DockerRequirement:
    dockerPull: ubuntu:20.04
  InlineJavascriptRequirement: {}
  InitialWorkDirRequirement:
    listing:
      - entryname: duplicate.txt
        writable: true # Unless writable we catch the problem due to overwriting a read-only file
        entry: |
          This is one file
      - entryname: unique.txt
        writable: true
        entry: |
          This is a different file
      - entryname: duplicate.txt
        writable: true
        entry: |
          This is another file pretending to be the first one

inputs: {}

stdout: stdout.txt

outputs:
  standard_out:
    type: stdout




