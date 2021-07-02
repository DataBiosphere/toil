cwlVersion: v1.2
class: Workflow
label: Molecular Dynamics Simulation.
doc: >
  CWL version of the md_list.cwl workflow for HPC. This performs a system setup and runs
  a molecular dynamics simulation on the structure passed to this workflow. This workflow
  uses the md_gather.cwl sub-workflow to gather the outputs together to return these.
  
  To work with more than one structure this workflow can be called from either the
  md_launch.cwl workflow, or the md_launch_mutate.cwl workflow. These use scatter for
  parallelising the workflow. md_launch.cwl operates on a list of individual input molecule
  files. md_launch_mutate.cwl operates on a single input molecule file, and a list of
  mutations to apply to that molecule. Within that list of mutations, a value of 'WT' will
  indicate that the molecule should be simulated without any mutation being applied.

requirements:
  SubworkflowFeatureRequirement: {}
  MultipleInputFeatureRequirement: {}


inputs:
  step1_pdb_file:
    label: Input file
    doc: Molecule to process (PDB format)
    type: File
  step2_editconf_config:
    label: Editconf configuration dictionary
    type: string
  step4_grompp_genion_config:
    label: GROMACS grompp configuration dictionary
    type: string
  step5_genion_config:
    label: Genion configuration dictionary
    type: string

outputs:
  outfile:
    label: example output
    type: File
    outputSource: step5_genion/output_top_zip_file

steps:
  subworkflow_configure:
    in:
      step1_pdb_file: step1_pdb_file
    out: [output_top_zip_file, output_gro_file]
 
    run:
      class: Workflow
      inputs:
        step1_pdb_file: File
      outputs:
        output_top_zip_file:
          type: File
          outputSource: step1_pdb2gmx/output_top_zip_file
        output_gro_file:
          type: File
          outputSource: step2_editconf/output_gro_file

      steps:
        step1_pdb2gmx:
          label: Create Protein System Topology
          doc: https://biobb-md.readthedocs.io/en/latest/gromacs.html#module-gromacs.pdb2gmx
          requirements:
            ResourceRequirement:
              coresMax: 1
          run: https://raw.githubusercontent.com/bioexcel/biobb_adapters/533bb65a01525cf03ee7421f12702fc8867c56fd/biobb_adapters/cwl/biobb_md/gromacs/pdb2gmx.cwl
          in:
            input_pdb_path: step1_pdb_file
          out: [output_gro_file, output_top_zip_file]

        step2_editconf:
          label: Create Solvent Box
          doc: https://biobb-md.readthedocs.io/en/latest/gromacs.html#module-gromacs.editconf
          requirements:
            ResourceRequirement:
              coresMax: 1
          run: https://raw.githubusercontent.com/bioexcel/biobb_adapters/533bb65a01525cf03ee7421f12702fc8867c56fd/biobb_adapters/cwl/biobb_md/gromacs/editconf.cwl
          in:
            input_gro_path: step1_pdb2gmx/output_gro_file
          out: [output_gro_file]

  subworkflow_solv_genion:
    in:
      input_gro_file: subworkflow_configure/output_gro_file
      input_top_zip_file: subworkflow_configure/output_top_zip_file
      step4_grompp_genion_config: step4_grompp_genion_config
    out: [output_top_zip_file, output_tpr_file]
    
    run:
      class: Workflow
      inputs:
        input_gro_file: File
        input_top_zip_file: File
        step4_grompp_genion_config: string
      outputs:
        output_top_zip_file:
          type: File
          outputSource: step3_solvate/output_top_zip_file
        output_tpr_file:
          type: File
          outputSource: step4_grompp_genion/output_tpr_file
    
      steps:
        step3_solvate:
          label: Fill the Box with Water Molecules
          doc: https://biobb-md.readthedocs.io/en/latest/gromacs.html#module-gromacs.solvate
          requirements:
            ResourceRequirement:
              coresMax: 1
          run: https://raw.githubusercontent.com/bioexcel/biobb_adapters/533bb65a01525cf03ee7421f12702fc8867c56fd/biobb_adapters/cwl/biobb_md/gromacs/solvate.cwl
          in:
            input_solute_gro_path: input_gro_file
            input_top_zip_path: input_top_zip_file
          out: [output_gro_file, output_top_zip_file]

        step4_grompp_genion:
          label: Add Ions - part 1
          doc: https://biobb-md.readthedocs.io/en/latest/gromacs.html#module-gromacs.grompp
          requirements:
            ResourceRequirement:
              coresMax: 1
          run: https://raw.githubusercontent.com/bioexcel/biobb_adapters/533bb65a01525cf03ee7421f12702fc8867c56fd/biobb_adapters/cwl/biobb_md/gromacs/grompp.cwl
          in:
            config: step4_grompp_genion_config
            input_gro_path: step3_solvate/output_gro_file
            input_top_zip_path: step3_solvate/output_top_zip_file
          out: [output_tpr_file]

  step5_genion:
    label: Add Ions - part 2
    doc: https://biobb-md.readthedocs.io/en/latest/gromacs.html#module-gromacs.genion
    run: https://raw.githubusercontent.com/bioexcel/biobb_adapters/533bb65a01525cf03ee7421f12702fc8867c56fd/biobb_adapters/cwl/biobb_md/gromacs/genion.cwl
    in:
      config: step5_genion_config
      input_tpr_path: subworkflow_solv_genion/output_tpr_file
      input_top_zip_path: subworkflow_solv_genion/output_top_zip_file
    out: [output_gro_file, output_top_zip_file]
    








