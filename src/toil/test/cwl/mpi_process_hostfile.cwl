cwlVersion: v1.2
class: CommandLineTool
$namespaces:
  cwltool: "http://commonwl.org/cwltool#"
doc: Prints, from each of its processes, the process hostfile it was given.
requirements:
  cwltool:MPIRequirement:
    processes: 2
  ResourceRequirement:
    coresMin: 2
baseCommand: [sh, -c, 'cat "$TOIL_PROCESS_HOSTFILE"']
inputs: []
outputs:
  hosts:
    type: stdout
