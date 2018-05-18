#
# Simplest example command line program wrapper for the Unix tool "rev".
#
class: CommandLineTool
cwlVersion: v1.0
doc: "Reverse each line using the `rev` command"

requirements:
  InlineJavascriptRequirement: {}
  # A lengthy process to test premature execution of descendent steps
  ResourceRequirement:
    ramMin: |
      ${ var start = new Date().getTime();
         while (new Date().getTime() < start + 10000);
         return inputs.input.size + 1536;
       }

inputs:
  input:
    type: File
    inputBinding: {}

outputs:
  output:
    type: File
    outputBinding:
      glob: output.txt

baseCommand: rev

# Specify that the standard output stream must be redirected to a file called
# output.txt in the designated output directory.
stdout: output.txt
