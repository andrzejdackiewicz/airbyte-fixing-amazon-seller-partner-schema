#!/usr/bin/env bash

set -e

. tools/lib/lib.sh

assert_root

# Since KIND does not have access to the local docker agent, manually load the minimum images required for the Kubernetes Acceptance Tests.
# See https://kind.sigs.k8s.io/docs/user/quick-start/#loading-an-image-into-your-cluster.
echo "Loading images into KIND..."
kind load docker-image airbyte/server:0.27.2-alpha --name chart-testing
kind load docker-image airbyte/scheduler:0.27.2-alpha --name chart-testing
kind load docker-image airbyte/webapp:0.27.2-alpha --name chart-testing
kind load docker-image airbyte/seed:0.27.2-alpha --name chart-testing
kind load docker-image airbyte/db:0.27.2-alpha --name chart-testing

echo "Starting app..."

echo "Applying dev manifests to kubernetes..."
# TODO: revert to dev -- TEMPORARY FOR TESTING
kubectl apply -k kube/overlays/stable

kubectl wait --for=condition=Available deployment/airbyte-server --timeout=300s || (kubectl describe pods && exit 1)
kubectl wait --for=condition=Available deployment/airbyte-scheduler --timeout=300s || (kubectl describe pods && exit 1)

# allocates a lot of time to start kube. takes a while for postgres+temporal to work things out
sleep 120s

server_logs () { echo "server logs:" && kubectl logs deployment.apps/airbyte-server; }
scheduler_logs () { echo "scheduler logs:" && kubectl logs deployment.apps/airbyte-scheduler; }
pod_sweeper_logs () { echo "pod sweeper logs:" && kubectl logs deployment.apps/airbyte-pod-sweeper; }
describe_pods () { echo "describe pods:" && kubectl describe pods; }
print_all_logs () { server_logs; scheduler_logs; pod_sweeper_logs; describe_pods; }

trap "echo 'kube logs:' && print_all_logs" EXIT

kubectl port-forward svc/airbyte-server-svc 8001:8001 &

echo "Running worker integration tests..."
./gradlew --no-daemon :airbyte-workers:integrationTest --scan

echo "Running e2e tests via gradle..."
KUBE=true ./gradlew --no-daemon :airbyte-tests:acceptanceTests --scan
