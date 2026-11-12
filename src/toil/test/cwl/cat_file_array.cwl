#!/usr/bin/env cwl-runner
cwlVersion: v1.2
class: CommandLineTool
doc: >
  Concatenate an array of files in a container. The file paths reach the tool
  through a file in the working directory, so the only part of the container
  command that grows with the number of files is the mounts for them.
requirements:
  DockerRequirement:
    dockerPull: debian:12
  InlineJavascriptRequirement: {}
  InitialWorkDirRequirement:
    listing:
      - entryname: paths.txt
        entry: |-
          ${
            return inputs.files.map(function(f) { return f.path + "\n"; }).join("");
          }
inputs:
  files: File[]
baseCommand: [sh, -c, "xargs cat < paths.txt"]
stdout: concatenated.txt
outputs:
  concatenated:
    type: stdout
