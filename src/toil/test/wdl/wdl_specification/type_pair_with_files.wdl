workflow typePairWorkflow {
  # test conformance with the WDL language specification - Pair Literals

  # file import
  Pair[String, File] test_pair_file

  # file import with arrays
  Array[Pair[String, File]] test_array_pair_file

  call copy_output {
    input:
      test_pair_file=test_pair_file,
      test_array_pair_file=test_array_pair_file
  }
}

task copy_output {
  Pair[String, File] test_pair_file
  Array[Pair[String, File]] test_array_pair_file

  # pairs defined in WDL in task
  Array[Pair[String, File]] test_array_pair_file_from_wdl_in_task = [
    ('test_A', 'src/toil/test/wdl/testfiles/test_int.txt'),
    ('test_B', 'src/toil/test/wdl/testfiles/test_string.txt')]

  command {
    cp ${write_json([
                    read_lines(test_pair_file.right),
                    read_lines(select_first(test_array_pair_file).right),
                    read_lines(select_first(test_array_pair_file_from_wdl_in_task).right)])} output.txt
  }

 output {
    File the_output = 'output.txt'
 }
}
