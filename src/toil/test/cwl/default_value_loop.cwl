#!/usr/bin/env cwl-runner
cwlVersion: v1.2
class: Workflow
$namespaces:
  cwltool: "http://commonwl.org/cwltool#"
requirements:
  InlineJavascriptRequirement: {}
  ScatterFeatureRequirement: {}
  SubworkflowFeatureRequirement: {}
inputs:
  i1: int
outputs:
  o1:
    type: int[]
    outputSource: loop/o1
    pickValue: all_non_null
steps:
  loop:
    run:
      class: Workflow
      inputs:
        i1: int
      outputs:
        o1:
          type: int?
          outputSource: big_values/o1
      steps:
        big_values:
          when: $(inputs.i1 >= 5)
          run:
            class: ExpressionTool
            inputs:
              i1: int
            outputs:
              o1: int
            expression: >
              ${return {'o1': inputs.i1 + 3};}
          in:
            i1: i1
          out: [ o1 ]
    in:
      i1: i1
    out: [ o1 ]
    requirements:
      cwltool:Loop:
        loopWhen: $(inputs.i1 < 20)
        loop:
          i1:
            loopSource: o1
            default: 5
        outputMethod: all
