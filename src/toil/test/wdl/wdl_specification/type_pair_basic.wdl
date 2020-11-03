workflow typePairWorkflow {
  # test conformance with the WDL language specification - Pair Literals

  Pair[Int, String] test_pair

  # a pair inside a pair
  Pair[String, Pair[String, String]] test_pair_pair

  # a pair defined inside WDL
  Pair[Int, String] test_pair_from_wdl = (23, "twenty-three")

  call copy_output {
    input:
      test_pair=test_pair,
      test_pair_pair=test_pair_pair,
      test_pair_from_wdl=test_pair_from_wdl
  }
}

task copy_output {
  Pair[Int, String] test_pair
  Pair[String, Pair[String, String]] test_pair_pair
  Pair[Int, String] test_pair_from_wdl

  command {
    cp ${write_json([
                    test_pair.left,
                    test_pair.right,
                    test_pair_pair.right.right,
                    test_pair_from_wdl])} output.txt
  }

 output {
    File the_output = 'output.txt'
 }
}
