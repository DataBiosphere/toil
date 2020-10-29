workflow lengthWorkflow {
  # this workflow should throw an error. length() does not work with Map[X, Y].
  Map[String, String] in_map
  call copy_output {input: num=length(in_map)}
}

task copy_output {
  Int num

  command {
    echo ${num} > output.txt
  }

  output {
    File the_output = 'output.txt'
  }
}
