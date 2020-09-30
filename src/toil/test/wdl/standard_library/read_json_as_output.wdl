workflow readJsonWorkflow {
  # this workflow depends on stdout() and write_lines()
  File in_file

  call read_json {input: in_file=in_file}
  call copy_output {input: in_json=read_json.out_json}
}

task read_json {
  File in_file

  command {
    cat ${in_file}
  }

  output {
    Map[String, String] out_json = read_json(stdout())
  }
}

task copy_output {
  Map[String, String] in_json

  command {
    cp ${write_json(in_json)} output.txt
  }

  output {
    File the_output = 'output.txt'
  }
}
