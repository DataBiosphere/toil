task t1 {
  File i
  String pattern
  command { grep '${pattern}' ${i} > "filtered" }
  output { File filtered = "filtered" }
}

task t2 {
  File i
  Array[String] s = ["a", "b", "c"]
  command {
    cat ${i} > out_file
    echo -e "${sep="\\n" s}" >> out_file
  }
  output { Array[String] strings = read_lines("out_file") }
}

task t3 {
  Array[String] strings
  command {echo "${sep="," strings}"}
  output { File concat = stdout() }
}

workflow w {
  call t1
  call t2 {
    input: i=t1.filtered
  }
  scatter(x in t2.strings) {
    call t3 {
      input: strings=t2.strings
    }
  }
}
