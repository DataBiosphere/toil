workflow floorWorkflow {
  Float num
  call get_floor { input: num=floor(num) }
}

task get_floor {
  Float num

  command {
    echo ${num} > output.txt
  }

 output {
    File the_flooring = 'output.txt'
 }
}
