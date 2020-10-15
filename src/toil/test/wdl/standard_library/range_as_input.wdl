workflow rangeWorkflow {
  # this workflow depends on write_lines()
  Int num
  call copy_output {input: in_array=range(num)}
}

task copy_output {
  Array[Int] in_array

  command {
    cp ${write_lines(in_array)} output.txt
  }

  output {
    File the_output = 'output.txt'
  }
}
