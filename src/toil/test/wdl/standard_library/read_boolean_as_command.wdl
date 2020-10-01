workflow readBooleanWorkflow {
  File in_file

  call read_boolean {input: in_file=in_file}
}

task read_boolean {
  File in_file

  command {
    echo "${read_boolean(in_file)}" > output.txt
  }

  output {
    File the_output = 'output.txt'
  }
}
