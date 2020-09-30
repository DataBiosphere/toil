workflow roundWorkflow {
  Float num
  call get_round { input: num=num }
}

task get_round {
  Float num

  command {
    echo ${round(num)} > output.txt
  }

 output {
    File the_rounding = 'output.txt'
 }
}
