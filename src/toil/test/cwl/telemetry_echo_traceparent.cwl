cwlVersion: v1.2
class: CommandLineTool
doc: Prints its input and the TRACEPARENT it sees.
baseCommand: [sh, -c, 'echo "$0 TRACEPARENT=${TRACEPARENT:-none}"']
inputs:
  x: {type: string, inputBinding: {position: 1}}
outputs:
  out:
    type: string
    outputBinding:
      glob: o.txt
      loadContents: true
      outputEval: $(self[0].contents)
stdout: o.txt
requirements: {InlineJavascriptRequirement: {}}
