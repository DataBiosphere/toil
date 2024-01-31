cwlVersion: v1.2
class: CommandLineTool
baseCommand:
  - python
  - copier.py

requirements:
  - class: InlineJavascriptRequirement
  - class: InitialWorkDirRequirement
    listing:
      - entryname: copier.py
        entry: |
          import os, subprocess, sys
          msin = sys.argv[1]
          root = os.path.basename(msin) + '.sector'
          for i in range(2):
            msout = f"{root}_{i}"
            print(f"Copying: {msin} -> {msout}")
            subprocess.check_call(['cp', '-R', '-L', msin, msout])

inputs:
  - id: msin
    type: Directory
    inputBinding:
      position: 0

outputs:
  - id: msout
    type: Directory[]
    outputBinding:
      glob: ['$(inputs.msin.basename).sector_*']
