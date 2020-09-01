workflow ceilWorkflow {
  Float num
  call get_ceil { input: num=num }
}

task get_ceil {
  Float num

  command {
    echo ${ceil(num)} > output.txt
  }

 output {
    File the_ceiling = 'output.txt'
 }
}
