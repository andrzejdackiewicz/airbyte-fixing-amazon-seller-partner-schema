# Intentionally no set -e, because we want to run normalization even if the destination fails
set -o pipefail

/airbyte/base.sh $@
destination_exit_code=$?

if test "$1" = 'write' && test "$NORMALIZATION_TECHNIQUE" = 'LEGACY'
then
  echo '{"type": "LOG","log":{"level":"INFO","message":"Starting in-connector normalization"}}'
  # the args in a write command are `write --catalog foo.json --config bar.json`
  # so if we remove the `write`, we can just pass the rest directly into normalization
  /airbyte/entrypoint.sh run ${@:2} --integration-type bigquery | java -cp "/airbyte/lib/*" io.airbyte.integrations.destination.normalization.NormalizationLogParser
  normalization_exit_code=$?
  echo '{"type": "LOG","log":{"level":"INFO","message":"Completed in-connector normalization"}}'
else
  echo '{"type": "LOG","log":{"level":"INFO","message":"Skipping in-connector normalization"}}'
  normalization_exit_code=0
fi

if test destination_exit_code -eq 0 && normalization_exit_code -eq 0
then
  exit 0
else
  exit 1
fi
