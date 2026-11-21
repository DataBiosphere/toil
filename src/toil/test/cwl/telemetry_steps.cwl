cwlVersion: v1.2
class: Workflow
doc: A top-level step, a step in a sub-workflow, and a scattered step.
requirements: {SubworkflowFeatureRequirement: {}, ScatterFeatureRequirement: {}}
inputs: {}
outputs:
  a: {type: string, outputSource: s1/out}
  b: {type: string, outputSource: outer/out}
  c: {type: 'string[]', outputSource: sc/out}
steps:
  s1:
    run: telemetry_echo_traceparent.cwl
    in: {x: {default: one}}
    out: [out]
  outer:
    run:
      class: Workflow
      inputs: {}
      outputs: {out: {type: string, outputSource: inner/out}}
      steps:
        inner:
          run: telemetry_echo_traceparent.cwl
          in: {x: {default: two}}
          out: [out]
    in: {}
    out: [out]
  sc:
    run: telemetry_echo_traceparent.cwl
    scatter: x
    in: {x: {default: [p, q, r]}}
    out: [out]
