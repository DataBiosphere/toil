cwlVersion: v1.2
class: Workflow

requirements:
  SubworkflowFeatureRequirement: {}

inputs:
  - id: input_directory
    type: Directory
  - id: input_filename
    type: string
    
steps:
  
  file_from_directory:
    run: file_from_directory.cwl
    in:
      dir: input_directory
      filename: input_filename
    out: [file]
    
  check_from_directory:
    run: download.cwl
    in:
      input: file_from_directory/file
    out: [output]
    
outputs:
  - id: output
    type: File
    outputSource: check_from_directory/output
