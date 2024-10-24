# This workflow fills in a required int from an optional int, but only when the
# int is really present. But it also uses the value to compute the conditional
# task's resource requirements, so Toil can't just schedule the task and then
# check the condition.
# See <https://github.com/DataBiosphere/toil/issues/4930#issue-2297563321>
cwlVersion: v1.2
class: Workflow
requirements:
  InlineJavascriptRequirement: {}
inputs:
  optional_input: int?
steps:
  the_step:
    in:
      required_input:
        source: optional_input
    when: $(inputs.required_input != null)
    run:
      cwlVersion: v1.2
      class: CommandLineTool
      inputs:
        required_input: int
      requirements:
        ResourceRequirement:
          coresMax: $(inputs.required_input)
      baseCommand: "nproc"
      outputs: []
    out: []
outputs: []
