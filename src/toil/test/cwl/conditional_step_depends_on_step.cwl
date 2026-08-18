# `consume`'s `when` references `produce`'s output, so the condition can only
# be evaluated once that output's promise has resolved.
# See <https://github.com/DataBiosphere/toil/issues/3990>.
cwlVersion: v1.2
class: Workflow
requirements:
  InlineJavascriptRequirement: {}
inputs:
  sleep: int
outputs: []
steps:
  produce:
    in:
      sleep: sleep
    out: [result]
    run:
      cwlVersion: v1.2
      class: ExpressionTool
      requirements:
        InlineJavascriptRequirement: {}
      inputs:
        sleep: int
      outputs:
        result: int
      expression: "$({'result': inputs.sleep})"
  consume:
    in:
      result: produce/result
    when: $(inputs.result > 1)
    run:
      cwlVersion: v1.2
      class: CommandLineTool
      inputs:
        result: int
      baseCommand: "true"
      outputs: []
    out: []
