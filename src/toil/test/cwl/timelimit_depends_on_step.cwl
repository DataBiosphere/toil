# `consume`'s ToolTimeLimit references `produce`'s output, so the time limit
# can only be evaluated once that output's promise has resolved.
cwlVersion: v1.2
class: Workflow
requirements:
  InlineJavascriptRequirement: {}
inputs:
  seconds: int
outputs: []
steps:
  produce:
    in:
      seconds: seconds
    out: [result]
    run:
      cwlVersion: v1.2
      class: ExpressionTool
      requirements:
        InlineJavascriptRequirement: {}
      inputs:
        seconds: int
      outputs:
        result: int
      expression: "$({'result': inputs.seconds})"
  consume:
    in:
      seconds: produce/result
    run:
      cwlVersion: v1.2
      class: CommandLineTool
      requirements:
        ToolTimeLimit:
          timelimit: $(inputs.seconds)
      inputs:
        seconds: int
      baseCommand: "true"
      outputs: []
    out: []
