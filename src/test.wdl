workflow w {
  String test_str
  String? test_str_opt
  Array[String]? test_arr_opt
  Array[Array[String?]?]? test_arr_arr_opt

  Map[String, Array[Pair[Int, Array[File]]]] complex
}
