$namespaces:
  arv: "http://arvados.org/cwl#"

class: CommandLineTool
doc: "Be preemptible"
cwlVersion: v1.0

inputs:
  - id: preemptible
    type: boolean

outputs:
  output:
    type: File
    outputBinding:
      glob: output.txt

requirements:
  InlineJavascriptRequirement: {}

hints:
  arv:UsePreemptible:
    # This isn't allowed; this always has to be a real boolean and can't be a cwl:Expression. See
    # https://github.com/arvados/arvados/blob/48a0d575e6de34bcda91c489e4aa98df291a8cca/sdk/cwl/arvados_cwl/arv-cwl-schema-v1.1.yml#L345
    usePreemptible: $(inputs.preemptible)

baseCommand: ["echo", "hello"]
stdout: output.txt
