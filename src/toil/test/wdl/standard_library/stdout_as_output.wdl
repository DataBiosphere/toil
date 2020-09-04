workflow stdoutWorkflow {
  String message
  call get_stdout { input: message=message }
  call copy_output { input: in_file=get_stdout.check_this }
}

task get_stdout {
  String message

  command {
    echo "${message}"
  }

  output {
    File check_this = stdout()
 }
}

# comply with builtinTest
task copy_output {
  File in_file

  command {
    cp ${in_file} output.txt
  }

  output {
    File the_output = 'output.txt'
  }
}
