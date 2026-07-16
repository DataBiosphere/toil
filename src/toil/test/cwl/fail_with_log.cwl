cwlVersion: v1.2
class: Workflow

inputs:
  - id: text
    type: string
    default: "This is a test"

steps:
  hello:
    run:
      class: CommandLineTool
      inputs:
        s: string
      baseCommand: [bash]
      arguments: [ "-c", "echo >&2 '$(inputs.s)' ; exit 1" ]
      stdout: output_log.txt
      stderr: error_log_secret.txt
      outputs:
        out_file:
          type: File
          outputBinding:
            glob: output_log.txt
        err_file:
          type: File
          outputBinding:
            glob: error_log.txt
    in:
      - id: s
        source: text
    out:
      - id: out_file
      - id: err_file

  count:
    run:
      class: CommandLineTool
      inputs:
        in_file: File
      baseCommand: [wc]
      arguments: [ "-l", $(inputs.in_file) ]
      stdout: count.txt
      stderr: error_log.txt
      outputs:
        out_file:
          type: File
          outputBinding:
            glob: count.txt
        err_file:
          type: File
          outputBinding:
            glob: error_log.txt
    in:
      - id: in_file
        source: hello/out_file
    out:
      - id: out_file
      - id: err_file

outputs:
  - id: hello_log
    type: File
    outputSource: hello/err_file
  - id: count_log
    type: File
    outputSource: count/err_file
  - id: count_result
    type: File
    outputSource: count/out_file

