$namespaces:
  arv: "http://arvados.org/cwl#"

class: CommandLineTool
doc: "Be preemptible"
cwlVersion: v1.0

inputs: []

outputs:
  output:
    type: File
    outputBinding:
      glob: output.txt

hints:
  arv:UsePreemptible:
    usePreemptible: true

baseCommand: ["echo", "hello"]
stdout: output.txt
