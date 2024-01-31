cwlVersion: v1.2
class: CommandLineTool
inputs: []
outputs:
  shouldmake:
    type: Directory
    outputBinding:
      glob: "shouldmake"
baseCommand: [ bash, -c ]
arguments:
 - |
   mkdir -p shouldmake;
   touch shouldmake/test.txt;


