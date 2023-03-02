version 1.0

task md5 {
  input {
    File inputFile
  }
  command {
    /bin/my_md5sum ${inputFile}
  }

 output {
    File value = "md5sum.txt"
 }

 runtime {
   docker: "quay.io/briandoconnor/dockstore-tool-md5sum:1.0.4"
   cpu: 1
   memory: "512 MB"
   disks: "local-disk 10 HDD"
 }
}

workflow ga4ghMd5 {
  input {
    File inputFile
  }
  call md5 { input: inputFile=inputFile }
  output {
    File value = md5.value
  }
}
