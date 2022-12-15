# Example CUDARequirement test tool from https://github.com/common-workflow-language/cwltool/pull/1581#issue-1087165113
cwlVersion: v1.2
class: CommandLineTool
baseCommand: nvidia-smi
inputs: []
outputs: []
$namespaces:
  cwltool: http://commonwl.org/cwltool#
hints:
  cwltool:CUDARequirement:
    cudaVersionMin: "11.4"
    cudaComputeCapabilityMin: "3.0"
    deviceCountMin: 1
    deviceCountMax: 8
  DockerRequirement:
    dockerPull: nvidia/cuda:11.4.0-base-ubuntu20.04
