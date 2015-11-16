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
    cat ${i}
    echo "${sep="," s}"
  }
  output { Array[String] strings = read_lines(stdout()) }
}

task t3 {
  String x
  command {echo ${x}}
  output { String y = read_string(stdout()) }
}

workflow w {
  Array[File] files = ['a.txt', 'b.txt']
  call t1
  call t2 {input: i=t1.filtered}
  #scatter(x in t2.strings) {
  #  call t3 {
  #    input: x=x
  #  }
  #}
}
