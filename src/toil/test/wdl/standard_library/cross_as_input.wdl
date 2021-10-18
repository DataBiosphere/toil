workflow crossWorkflow {
  # this workflow depends on write_json()
  Array[Int] in_array_1
  Array[String] in_array_2

  call copy_output {input: in_array=cross(in_array_1, in_array_2)}
}

task copy_output {
  Array[Pair[Int, String]] in_array

  command {
    cp ${write_json(in_array)} output.txt
  }

  output {
    File the_output = 'output.txt'
  }
}
