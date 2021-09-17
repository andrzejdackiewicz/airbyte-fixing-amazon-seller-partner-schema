#!/bin/bash

set -e

export RELEASE_NAME="${RELEASE_NAME:-airbyte}"
export NAMESPACE="${NAMESPACE:-airbyte}"
export INSTALL_TIMEOUT="${INSTALL_TIMEOUT:-600s}"

usage() {
  echo "Airbyte Helm Chart CI Script"
  echo ""
  echo "Usage:"
  echo "  ./ci.sh [command]"
  echo ""
  echo "Available Commands"
  echo ""
  echo "lint                    Lints the helm chart"
  echo "test                    Runs 'helm test' for the helm chart"
  echo "install                 Installs the helm chart"
  echo "diagnostics             Prints diagnostics & logs of pods that aren't running"
  echo "template                Templates out the default kubernetes manifests generated by helm"
  echo "update-docs             Regenerates the README.md documentation after making changes to the values.yaml"
  echo "check-docs-updated      Fails if changes values.yaml and README.md documentation are out of sync"
  echo "help                    Displays help about this command"
}

if ! helm repo list | grep "bitnami" > /dev/null 2>&1; then
  helm repo add bitnami https://charts.bitnami.com/bitnami
fi

if [ ! -d ./charts ]; then
  helm dep build
fi


case "$1" in
  lint)
    helm lint .
    ;;

  test)
    helm test "${RELEASE_NAME}" --logs --debug --namespace "${NAMESPACE}"
    ;;

  install)
    helm upgrade --install \
      --create-namespace \
      --namespace "${NAMESPACE}" \
      --debug \
      --wait \
      --timeout "${INSTALL_TIMEOUT}" \
      "${RELEASE_NAME}" .
    ;;

  diagnostics)
    kubectl -n "${NAMESPACE}" get pods -o wide
	  kubectl -n "${NAMESPACE}" get pods --no-headers | grep -v "Running" | awk '{print $1}' | xargs -L 1 -r kubectl -n "${NAMESPACE}" logs
    ;;

  template)
    helm template --release-name "${RELEASE_NAME}" .
    ;;

  # Uses the readme-generator command to update the README.md
  # with changes in the values.yaml file. See: https://github.com/bitnami-labs/readme-generator-for-helm
  update-docs)
    readme-generator -v ./values.yaml -r README.md
    ;;

  # Checks if the docs were updated as a result of changes in the values.yaml file and if they were
  # fails CI with a message about needing to run the update-docs command.
  check-docs-updated)
    readme-generator -v ./values.yaml -r README.md
    if git status --porcelain | grep "charts/airbyte/README.md"; then
      echo "It appears the README.md has not been updated with changes from the values.yaml."
      echo "Please run './ci.sh update-docs' locally and commit your changes."
      exit 1
    else
      echo "No uncommitted changes to the README.md detected"
    fi
    ;;

  help)
    usage
    ;;

  *)
    usage
    ;;
esac
