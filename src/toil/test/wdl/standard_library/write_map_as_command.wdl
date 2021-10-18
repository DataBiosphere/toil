workflow writeMapWorkflow {
  Map[String, String] in_map

  call write_map {input: in_map=in_map}
}

task write_map {
  Map[String, String] in_map

  command {
    cp ${write_map(in_map)} output.txt
  }

  output {
    File the_output = 'output.txt'
  }
}
