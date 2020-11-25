workflow sizeWorkflow {
  call get_size
  call copy_output {
    input:
      float_0 = get_size.created_file_size,
      float_1 = get_size.created_file_size_in_KB,
      float_2 = get_size.created_file_size_in_KiB
  }
}

task get_size {

  command {
    echo "this file is 22 bytes" > created_file
  }

  output {
    Float created_file_size = size("created_file") # 22.0
    Float created_file_size_in_KB = size("created_file", "K") # 0.022
    Float created_file_size_in_KiB = size("created_file", "Ki") # 0.021484375
  }
}

task copy_output {
  Float float_0
  Float float_1
  Float float_2

  command {
    echo "${float_0} ${float_1} ${float_2}" > output.txt
  }

  output {
    File the_output = 'output.txt'
  }
}
