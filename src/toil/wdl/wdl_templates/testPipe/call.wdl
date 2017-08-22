workflow write_simple_file {
  call write_file
}

task write_file {
  String password
  command { echo ${password} > test.txt }
  output { File test = "'test.txt'" }
}
