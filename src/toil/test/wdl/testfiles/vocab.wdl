workflow vocabulary {

  #####################################################################
  # $primitive_type = ('Boolean' | 'Int' | 'Float' | 'File' | 'String')
  Boolean bool1
  Int int1
  Float float1
  File file1
  String string1

  Boolean bool2 = true
  Int int2 = 1
  Float float2 = 1.1
  File file2 = 'src/toil/test/wdl/test.tsv'
  String string2 = 'x'

  Boolean bool3 = true
  Int int3 = 1
  Float float3 = 1.1
  File file3 = 'src/toil/test/wdl/test.csv'
  String string3 = 'x'

  #####################################################################
  # $array_type = 'Array' '[' ($primitive_type | $object_type | $array_type) ']'
  Array[Boolean] arraybool1
  Array[Int] arrayint1
  Array[Float] arrayfloat1
  Array[File] arrayfile1
  Array[String] arraystring1

  Array[Array[Boolean]] arrayarraybool1
  Array[Array[Int]] arrayarrayint1
  Array[Array[Float]] arrayarrayfloat1
  Array[Array[File]] arrayarrayfile1
  Array[Array[String]] arrayarraystring1

  #####################################################################
  # $type_postfix_quantifier = '?' | '+'
  String joinedstring = string2 + string3 + "x"
  Int joinedint = int2 + int3 + 1
  Float joinedfloat = float2 + float3 + 1.1

  Boolean? boolo1
  Int? into1
  Float? floato1
  File? fileo1
  String? stringo1

  Boolean? boolo2 = true
  Int? into2 = 1
  Float? floato2 = 1.1
  File? fileo2 = 'src/toil/test/wdl/test.csv'
  String? stringo2 = 'x'

  #####################################################################
  # $object_type = 'Object'
  # $map_type = 'Map' '[' $primitive_type ',' ($primitive_type | $array_type | $map_type | $object_type) ']'
  # currently unsupported

  #####################################################################
  # demonstrate function usage
  Int additional_disk = select_first([into1, 20])
  Int num_of_bqsr_scatters = length([1,1,1,1])
  Int bqsr_divisor = if int3 > 1 then additional_disk else 1
  Float disk_size = (floato2 / bqsr_divisor) + additional_disk
}
