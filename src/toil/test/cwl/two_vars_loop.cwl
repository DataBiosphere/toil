#!/usr/bin/env cwl-runner
cwlVersion: v1.2
class: Workflow
$namespaces:
  cwltool: "http://commonwl.org/cwltool#"
requirements:
  InlineJavascriptRequirement: {}
inputs:
  i1: int
  i2: int
outputs:
  o1:
    type: int
    outputSource: subworkflow/o1
steps:
  subworkflow:
    run:
      class: ExpressionTool
      inputs:
        i1: int
        i2: int
      outputs:
        o1: int
        o2: int
      expression: >
        ${return {'o1': inputs.i1 + inputs.i2, 'o2': inputs.i2};}
    in:
      i1: i1
      i2: i2
    out: [o1, o2]
    requirements:
      cwltool:Loop:
        loopWhen: $(inputs.i1 < 10)
        loop:
          i1: o1
          i2: o2
        outputMethod: last
