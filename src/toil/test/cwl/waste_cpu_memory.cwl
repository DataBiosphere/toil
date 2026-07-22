cwlVersion: v1.2
class: CommandLineTool

doc: "Burn ~30s of CPU and allocate ~1 GiB of RAM inside a container, for stats monitoring tests."

requirements:
  - class: DockerRequirement
    dockerPull: python:3-slim

baseCommand:
  - python3
  - -c
  - |
    import time
    # Hold ~1 GiB in RAM for the duration of the run.
    data = bytearray(1024 * 1024 * 1024)
    end = time.monotonic() + 30
    while time.monotonic() < end:
        pass

inputs: []
outputs: []
