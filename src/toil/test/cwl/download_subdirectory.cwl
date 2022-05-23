cwlVersion: v1.2
class: Workflow

requirements:
  SubworkflowFeatureRequirement: {}

inputs:
  - id: input_parent_directory
    type: Directory
  - id: input_subdirname
    type: string
  - id: input_filename
    type: string
    
steps:
  
  directory_from_directory:
    run: directory_from_directory.cwl
    in:
      dir: input_parent_directory
      filename: input_subdirname
    out: [directory]
    
  file_from_subdirectory:
    run: file_from_directory.cwl
    in:
      dir: directory_from_directory/directory
      filename: input_filename
    out: [file]
    
  check_from_subdirectory:
    run: download.cwl
    in:
      input: file_from_subdirectory/file
    out: [output]
      
outputs:
  - id: output
    type: File
    outputSource: check_from_subdirectory/output
