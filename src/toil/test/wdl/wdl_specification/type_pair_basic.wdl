workflow typePairWorkflow {
  # test conformance with the WDL language specification - Pair Literals

  Pair[String, File] test_pair_from_json
  Pair[String, File] test_pair = ('test_file', 'src/toil/test/wdl/testfiles/test.json')

  Pair[String, Pair[File, File]] test_pair_pair_from_json
  Pair[String, Pair[File, File]] test_pair_pair = ('test_file', ('src/toil/test/wdl/test.tsv', 'src/toil/test/wdl/test.csv'))

  # TODO: this probably can't be tested
  Pair[String, File]? test_pair_optional
  Pair[String, Pair[File, File]]? test_pair_pair_optional

  call copy_output {
    input:
      test_pair_from_json=test_pair_from_json,
      test_pair=test_pair,
      test_pair_pair_from_json=test_pair_pair_from_json,
      test_pair_pair=test_pair_pair
  }
}

task copy_output {
  Pair[String, File] test_pair_from_json
  Pair[String, File] test_pair

  Pair[String, Pair[File, File]] test_pair_pair_from_json
  Pair[String, Pair[File, File]] test_pair_pair

  command {
    cp ${write_json([
                      read_lines(test_pair_from_json.right),
                      read_lines(test_pair.right),
                      read_lines(test_pair_pair_from_json.right.right),
                      read_lines(test_pair_pair.right.right)])} output.txt
  }

 output {
    File out = 'output.txt'
 }
}
