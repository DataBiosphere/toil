version 1.0

workflow write_simple_file {
  input {
    String message
  }
  call write_file { input: message = message }
}

task write_file {
  input {
    String message
  }
  command { echo ~{message} > wdl-helloworld-output.txt }
  output { File test = "wdl-helloworld-output.txt" }
}
