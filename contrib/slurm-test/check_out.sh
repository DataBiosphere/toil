#!/bin/bash
if diff "output_Docker.txt" "expected_out.txt" >/dev/null; then
  echo "Output files are the same"
else
  echo "Failed, output of workflow different from expected_out"
  diff -c "output_Docker.txt" "expected_out.txt"
  exit 1
fi
rm output_Docker.txt