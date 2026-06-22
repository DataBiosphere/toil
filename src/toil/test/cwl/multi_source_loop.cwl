#!/usr/bin/env cwl-runner
cwlVersion: v1.2
class: Workflow
$namespaces:
  cwltool: "http://commonwl.org/cwltool#"
requirements:
  InlineJavascriptRequirement: {}
  MultipleInputFeatureRequirement: {}
  ScatterFeatureRequirement: {}
  SubworkflowFeatureRequirement: {}
inputs:
  i1: int
outputs:
  o1:
    type: int[]
    outputSource: [loop/osmall, loop/obig]
    linkMerge: merge_flattened
    pickValue: all_non_null
steps:
  loop:
    run:
      class: Workflow
      inputs:
        i1: int
      outputs:
        osmall:
          type: int?
          outputSource: small_values/o1
        obig:
          type: int?
          outputSource: big_values/o1
      steps:
        small_values:
          when: $(inputs.i1 < 5)
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
    out: [osmall, obig]
    requirements:
      cwltool:Loop:
        loopWhen: $(inputs.i1 < 20)
        loop:
          i1:
            loopSource: [osmall, obig]
            pickValue: the_only_non_null
        outputMethod: all
