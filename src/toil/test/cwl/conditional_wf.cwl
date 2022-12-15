class: Workflow
cwlVersion: v1.2

inputs:
  message: string
  sleep: int
outputs:
  out1:
    type: File
    outputSource:
      - echo/echoOut
    pickValue: first_non_null

requirements:
  InlineJavascriptRequirement: {}

steps:
  echo:
    in:
      message: message
      sleep: sleep
    run: echo.cwl
    out: [echoOut]
    when: $(inputs.sleep > 1)

