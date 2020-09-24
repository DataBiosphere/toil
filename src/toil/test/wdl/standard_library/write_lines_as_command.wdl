workflow writeLinesWorkflow {
  Array[String] in_array

  call write_lines {input: in_array=in_array}
}

task write_lines {
  Array[String] in_array

  command {
    cp ${write_lines(in_array)} output.txt
  }

  output {
    File the_output = 'output.txt'
  }
}
