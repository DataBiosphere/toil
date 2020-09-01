workflow ceilWorkflow {
  Float num
  call get_ceil { input: num=num }
}

task get_ceil {
  Float num

  command {
    echo ${ceil(num)} > the_ceiling.txt
  }

 output {
    File the_ceiling = 'the_ceiling.txt'
 }
}
