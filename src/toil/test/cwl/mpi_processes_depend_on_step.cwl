cwlVersion: v1.2
class: Workflow
$namespaces:
  cwltool: "http://commonwl.org/cwltool#"
requirements:
  InlineJavascriptRequirement: {}
inputs:
  ranks: int
outputs: []
steps:
  produce:
    in:
      ranks: ranks
    out: [result]
    run:
      cwlVersion: v1.2
      class: ExpressionTool
      requirements:
        InlineJavascriptRequirement: {}
      inputs:
        ranks: int
      outputs:
        result: int
      expression: "$({'result': inputs.ranks})"
  consume:
    in:
      ranks: produce/result
    run:
      cwlVersion: v1.2
      class: CommandLineTool
      requirements:
        cwltool:MPIRequirement:
          processes: $(inputs.ranks)
      inputs:
        ranks: int
      baseCommand: "true"
      outputs: []
    out: []
