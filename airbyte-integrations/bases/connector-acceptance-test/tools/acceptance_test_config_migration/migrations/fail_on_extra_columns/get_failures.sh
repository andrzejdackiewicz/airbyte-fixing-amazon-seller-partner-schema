#!/bin/bash
FULL_OUTPUT_DIR="./output/1"  # TODO: make this work even when called from another dir

grep -rnw "$FULL_OUTPUT_DIR" -e "Additional properties are not allowed" -B 1 > "/tmp/columns.txt"
mkdir -p "./test_failure_logs"

for f in $(ls $FULL_OUTPUT_DIR); do
  grep "/tmp/columns.txt" -e "$f" > "./test_failure_logs/$f"
done

rm /tmp/columns.txt
