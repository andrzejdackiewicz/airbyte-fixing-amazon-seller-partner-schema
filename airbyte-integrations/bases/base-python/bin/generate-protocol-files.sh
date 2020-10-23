#!/usr/bin/env bash

set -e

[ -z "$ROOT_DIR" ] && exit 1

YAML_DIR=airbyte-protocol/models/src/main/resources/airbyte_protocol
OUTPUT_DIR=airbyte-integrations/bases/base-python/airbyte_protocol/models

function main() {
  rm -rf "$ROOT_DIR/$OUTPUT_DIR"/*.py
  echo "# generated by generate-protocol-files" > "$ROOT_DIR/$OUTPUT_DIR"/__init__.py

  for f in "$ROOT_DIR/$YAML_DIR"/*.yaml; do
    filename_wo_ext=$(basename "$f" | cut -d . -f 1)
    echo "from .$filename_wo_ext import *" >> "$ROOT_DIR/$OUTPUT_DIR"/__init__.py

    docker run --user "$(id -u):$(id -g)" -v "$ROOT_DIR":/airbyte airbyte/code-generator:dev \
      --input "/airbyte/$YAML_DIR/$filename_wo_ext.yaml" \
      --output "/airbyte/$OUTPUT_DIR/$filename_wo_ext.py" \
      --disable-timestamp
  done
}

main "$@"
