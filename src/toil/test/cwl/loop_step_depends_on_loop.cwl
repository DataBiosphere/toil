# `after` reads the loop's output, so it must run after the loop's last
# iteration, not beside its first one.
cwlVersion: v1.2
class: Workflow
$namespaces: {cwltool: "http://commonwl.org/cwltool#"}
requirements: {InlineJavascriptRequirement: {}}
inputs: {}
outputs:
  final: {type: int, outputSource: after/out}
steps:
  count:
    in: {i: {default: 0}}
    out: [i]
    requirements:
      cwltool:Loop:
        loopWhen: $(inputs.i < 1)
        loop: {i: {loopSource: i}}
        outputMethod: last
    run:
      class: ExpressionTool
      inputs: {i: int}
      outputs: {i: int}
      expression: '$({"i": inputs.i + 1})'
  after:
    in: {i: count/i}
    out: [out]
    run:
      class: ExpressionTool
      inputs: {i: int}
      outputs: {out: int}
      expression: '$({"out": inputs.i})'
