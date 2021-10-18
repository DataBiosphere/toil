workflow readFloatWorkflow {
  File in_file

  call read_float {input: in_file=in_file}
}

task read_float {
  File in_file

  command {
    echo "${read_float(in_file)}" > output.txt
  }

  output {
    File the_output = 'output.txt'
  }
}
