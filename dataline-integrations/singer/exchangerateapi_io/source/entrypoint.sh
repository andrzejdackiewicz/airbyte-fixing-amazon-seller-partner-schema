#!/bin/bash

set -e

function echo2() {
  echo >&2 "$@"
}

function error() {
  echo2 "$@"
  exit 1
}

function main() {
  ARGS=
  while [ $# -ne 0 ]; do
    case "$1" in
    --discover)
      DISCOVER=1
      shift 1
      break
      ;;
    -b | --config)
      ARGS="$ARGS --config $2"
      shift 2
      ;;
    -c | --state)
      ARGS="$ARGS --state $2"
      shift 2
      ;;
    --catalog | --properties)
      # ignore
      shift 2
      ;;
    *)
      error "Unknown option: $1"
      shift
      ;;
    esac
  done

  # Singer's discovery is what we currently use to check connection
  if [ "$DISCOVER" == 1 ]; then
    echo2 "Checking connection..."
    echo '{"streams":[]}' > catalog.json
  else
    echo2 "Running sync..."
    tap-exchangeratesapi $ARGS
  fi
}

main "$@"
