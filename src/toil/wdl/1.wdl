task greeter {
  String name

  command {
    echo "Hello, ${name}"
  }
  output {
    String greeting = read_string(stdout())
    String test = "foo" + "bar " + read_string(stdout())
  }
}

task adder {
  Array[Int] numbers

  command {
    python -c "print(${sep='+' numbers})"
  }
  output {
    Int sum = read_int(stdout())
  }
}

workflow test {
  call greeter
  call adder
}
