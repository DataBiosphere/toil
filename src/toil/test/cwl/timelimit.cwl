class: CommandLineTool
cwlVersion: v1.2
inputs: []
outputs: []
requirements:
  InlineJavascriptRequirement: {}
  ToolTimeLimit:
    timelimit: $(3*4)
  WorkReuse:
    enableReuse: false
baseCommand: [sleep, "3"]
