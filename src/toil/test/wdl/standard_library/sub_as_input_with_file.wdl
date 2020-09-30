workflow subWorkflow {
  File in_file
  call get_sub {input: in_file=in_file}
}

task get_sub {
  File in_file
  String out_file_name = sub(in_file, "\\.tsv$", ".csv")

  command {
    echo "${out_file_name}" > output.txt
  }

  output {
    File out = 'output.txt'
  }
}
