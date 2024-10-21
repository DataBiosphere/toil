cwlVersion: v1.2
class: CommandLineTool
inputs: []
baseCommand: ["bash", "-c", "ulimit -m"]
stdout: memory.txt
outputs:
  memory:
    type: string
    outputBinding:
      glob: memory.txt
      loadContents: True
      outputEval: $(self[0].contents)
