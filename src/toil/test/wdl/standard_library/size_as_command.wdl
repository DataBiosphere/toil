workflow sizeWorkflow {
  File in_file

  call get_size {input: in_file=in_file}
}

task get_size {
  File in_file

  command {
    echo "${size(in_file)}" > output.txt
  }

  output {
    File the_output = 'output.txt'
  }
}
