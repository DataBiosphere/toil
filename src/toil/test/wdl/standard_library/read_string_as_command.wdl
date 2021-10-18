workflow readStringWorkflow {
  File in_file

  call read_string {input: in_file=in_file}
}

task read_string {
  File in_file

  command {
    echo "${read_string(in_file)}" > output.txt
  }

  output {
    File the_output = 'output.txt'
  }
}
