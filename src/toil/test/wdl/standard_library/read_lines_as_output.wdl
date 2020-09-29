workflow readLinesWorkflow {
  # this workflow depends on stdout() and write_lines()
  File in_file

  call read_lines {input: in_file=in_file}
  call copy_output {input: in_array=read_lines.out_array}
}

task read_lines {
  File in_file

  command {
    cat ${in_file}
  }

  output {
    Array[String] out_array = read_lines(stdout())
  }
}

task copy_output {
  Array[String] in_array

  command {
    cp ${write_lines(in_array)} output.txt
  }

  output {
    File the_output = 'output.txt'
  }
}
