#!/bin/bash
if diff "output_Docker.txt" "expected_out_basic.txt" >/dev/null; then
  echo "Output files are the same"
else
  echo "Failed, output of workflow different from expected_out"
  diff -c "output_Docker.txt" "expected_out.txt"
  exit 1
fi
if diff "expected_out_sort.txt" "sortedFile.txt" >/dev/null; then
  echo "Output files are the same"
else
  echo "Failed, output of workflow different from expected output"
  diff -c "expected_out_sort.txt" "sortedFile.txt"
  exit 1
fi
rm sortedFile.txt
rm output_Docker.txt