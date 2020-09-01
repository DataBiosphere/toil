workflow ceilWorkflow {
  Float num
  call get_ceil { input: num=ceil(num) }
}

task get_ceil {
  Float num

  command {
    echo ${num} > the_ceiling.txt
  }

 output {
    File the_ceiling = 'the_ceiling.txt'
 }
}
