cwlVersion: v1.2
class: CommandLineTool
baseCommand: [echo]
id: "echo"
inputs: 
  message: 
    type: string
    inputBinding:
      position: 1
outputs: 
  echoOut:
    type: stdout
