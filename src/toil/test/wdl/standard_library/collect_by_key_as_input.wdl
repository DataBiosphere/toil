version development

workflow collectByKeyWorkflow {
  input {
    Array[Pair[String, Int]] in_array
  }

  call copy_output {input: in_map=collect_by_key(in_array)}
}

task copy_output {
  input {
    Map[String, Array[Int]] in_map
  }

  command {
    cp ~{write_json(in_map)} output.txt
  }

  output {
    File the_output = 'output.txt'
  }
}

