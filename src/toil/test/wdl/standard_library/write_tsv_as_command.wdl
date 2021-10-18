workflow writeTsvWorkflow {
  Array[Array[String]] in_tsv

  call write_tsv {input: in_tsv=in_tsv}
}

task write_tsv {
  Array[Array[String]] in_tsv

  command {
    cp ${write_tsv(in_tsv)} output.txt
  }

  output {
    File the_output = 'output.txt'
  }
}
