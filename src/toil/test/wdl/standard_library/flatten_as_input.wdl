workflow flattenWorkflow {
  Array[Array[Int]] in_array

  call copy_output {input: in_array=flatten(in_array)}
}

task copy_output {

  Array[Int] in_array

  command {
    cp ${write_json(in_array)} output.txt
  }

  output {
    File the_output = 'output.txt'
  }
}
