workflow typePairWorkflow {
  # test conformance with the WDL language specification - Pair Literals

  Array[Pair[String, File]] test_array_pair_from_json
  Array[Pair[String, File]] test_array_pair = [('tsv_file', 'src/toil/test/wdl/test.tsv'), ('csv_file', 'src/toil/test/wdl/test.csv')]

  Array[Pair[String, Pair[File, File]]] test_array_pair_pair_from_json
  Array[Pair[String, Pair[File, File]]] test_array_pair_pair = [
    ('test_A', ('src/toil/test/wdl/test.tsv', 'src/toil/test/wdl/test.csv')),
    ('test_B', ('src/toil/test/wdl/testfiles/test_boolean.txt', 'src/toil/test/wdl/testfiles/test.json'))]

  call copy_output {
    input:
      test_array_pair_from_json=test_array_pair_from_json,
      test_array_pair=test_array_pair,
      test_array_pair_pair_from_json=test_array_pair_pair_from_json,
      test_array_pair_pair=test_array_pair_pair,
  }
}

task copy_output {
  Array[Pair[String, File]] test_array_pair_from_json
  Array[Pair[String, File]] test_array_pair

  Array[Pair[String, Pair[File, File]]] test_array_pair_pair_from_json
  Array[Pair[String, Pair[File, File]]] test_array_pair_pair

  # pairs defined in task should also work
  Array[Pair[String, Pair[File, File]]] test_array_pair_pair_from_json_in_task
  Array[Pair[String, Pair[File, File]]] test_array_pair_pair_in_task  = [
    ('test_A', ('src/toil/test/wdl/test.csv', 'src/toil/test/wdl/test.tsv')),
    ('test_B', ('src/toil/test/wdl/testfiles/test.json', 'src/toil/test/wdl/testfiles/test_boolean.txt'))]

  command {
    cp ${write_json([
                      test_array_pair_from_json,
                      test_array_pair,
                      test_array_pair_pair_from_json,
                      test_array_pair_pair,
                      test_array_pair_pair_from_json_in_task,
                      test_array_pair_pair_in_task])} output.txt
  }

 output {
    File out = 'output.txt'
 }
}
