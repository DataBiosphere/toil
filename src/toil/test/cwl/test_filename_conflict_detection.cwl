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
      # https://github.com/DataBiosphere/toil/issues/3449 is specifically about
      # a directory with a conflict in it so test that.
      - basename: subdir
        class: Directory
        listing:
          - basename: innderdupe.txt
            class: File
            contents: |
              Stuff
          - basename: innderdupe.txt
            class: File
            contents: |
              Other stuff

inputs: {}

stdout: stdout.txt

outputs:
  standard_out:
    type: stdout




