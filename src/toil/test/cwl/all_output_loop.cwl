#!/usr/bin/env cwl-runner
cwlVersion: v1.2
class: Workflow
$namespaces:
  cwltool: "http://commonwl.org/cwltool#"
requirements:
  InlineJavascriptRequirement: {}
inputs:
  i1: int
outputs:
  o1:
    type: int[]
    outputSource: subworkflow/o1
steps:
  subworkflow:
    run:
      class: ExpressionTool
      inputs:
        i1: int
      outputs:
        o1: int
      expression: >
        ${return {'o1': inputs.i1 + 1};}
    in:
      i1: i1
    out: [o1]
    requirements:
      cwltool:Loop:
        loopWhen: $(inputs.i1 < 10)
        loop:
          i1: o1
        outputMethod: all
