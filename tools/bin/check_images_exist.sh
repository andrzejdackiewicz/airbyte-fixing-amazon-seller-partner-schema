#!/usr/bin/env bash

# ------------- Import some defaults for the shell

# Source shell defaults
# $0 is the currently running program (this file)
this_file_directory=$(dirname $0)
relative_path_to_defaults=$this_file_directory/../shell_defaults

# if a file exists there, source it. otherwise complain
if test -f $relative_path_to_defaults; then
  # source and '.' are the same program
  source $relative_path_to_defaults
else
  echo -e "\033[31m\nFAILED TO SOURCE TEST RUNNING OPTIONS.\033[39m"
  echo -e "\033[31mTried $relative_path_to_defaults\033[39m"
  exit 1
fi

set +o xtrace  # +x easier human reading here

. tools/lib/lib.sh


function docker_tag_exists() {
  # Is true for images stored in the Github Container Registry
  repo=$1
  tag=$2
  # we user [[ here because test doesn't support globbing well
  if [[ $repo == ghcr* ]]
  then
    TOKEN_URL=https://ghcr.io/token\?scope\="repository:$1:pull"
    token=$(curl $TOKEN_URL | jq -r '.token' > /dev/null)
    URL=https://ghcr.io/v2/$1/manifests/$2
    echo -e "$blue_text""\n\n\tURL: $URL""$default_text"
    curl -H "Authorization: Bearer $token" --location --silent --show-error --dump-header header.txt "$URL" > /dev/null
    curl_success=$?
  else
    URL=https://hub.docker.com/v2/repositories/"$1"/tags/"$2"
    echo -e "$blue_text""\n\n\tURL: $URL""$default_text"
    curl --silent --show-error --location --dump-header header.txt "$URL" > /dev/null
    curl_success=$?
    # some bullshit to get the number out of a header that looks like this
    # < content-length: 1039
    # < x-ratelimit-limit: 180
    # < x-ratelimit-reset: 1665683196
    # < x-ratelimit-remaining: 180
    docker_rate_limit_remaining=$(grep 'x-ratelimit-remaining: ' header.txt | grep --only-matching --extended-regexp "\d+")
    # too noisy when set to < 1.  Dockerhub starts complaining somewhere around 10
    if test "$docker_rate_limit_remaining" -lt 20; then
      echo -e "$red_text""We are close to a sensitive dockerhub rate limit!""$default_text"
      echo -e "$red_text""SLEEPING 60s sad times""$default_text"
      sleep 60
      docker_tag_exists $1 $2
    elif test $docker_rate_limit_remaining -lt 50; then
      echo -e "$red_text""Rate limit reported as $docker_rate_limit_remaining""$default_text"
    fi
  fi
  if test $curl_success -ne 0; then
    echo -e "$red_text""Curl Said this didn't work.  Please investigate""$default_text"
    exit 1
  fi
}

checkPlatformImages() {
  echo -e "$blue_text""Checking platform images exist...""$default_text"
  #Pull without printing progress information and send error stream because that where image names are
  docker-compose pull --quiet 2> compose_output
  docker_compose_success=$?
  # quiet output is just SHAs ie: f8a3d002a8a6
  images_pulled_count=$(docker images --quiet | wc -l)
  if test $images_pulled_count -eq 0; then
    echo -e "$red_text""Nothing was pulled!  This script may be broken! We expect to pull something""$default_text"
    exit 1
  elif test $docker_compose_success -eq 0; then
    echo -e "$blue_text""Docker successfully pulled all images""$default_text"
  else
    echo -e "$red_text""docker-compose failed to pull all images""$default_text"
    cat compose_output
    exit 1
  fi
}

checkNormalizationImages() {
  echo -e "$blue_text""Checking Normalization images exist...""$default_text"
  # the only way to know what version of normalization the platform is using is looking in NormalizationRunnerFactory.
  local image_version;
  factory_path=airbyte-commons-worker/src/main/java/io/airbyte/workers/normalization/NormalizationRunnerFactory.java
  # -f True if file exists and is a regular file
  if ! test -f $factory_path; then
    echo -e "$red_text""No NormalizationRunnerFactory found at path! H4LP!!!""$default_text"
  fi
  image_version=$(cat $factory_path | grep 'NORMALIZATION_VERSION =' | cut -d"=" -f2 | sed 's:;::' | sed -e 's:"::g' | sed -e 's:[[:space:]]::g')
  echo -e "$blue_text""Checking normalization images with version $image_version exist...""$default_text"
  VERSION=$image_version docker-compose --file airbyte-integrations/bases/base-normalization/docker-compose.yaml pull --quiet
  docker_compose_success=$?
  if test $docker_compose_success -eq 0; then
    echo -e "$blue_text""Docker successfully pulled all images for normalization""$default_text"
  else
    echo -e "$red_text""docker-compose failed to pull all images for normalization""$default_text"
    exit 1
  fi
}

checkConnectorImages() {
  echo -e "$blue_text""Checking connector images exist...""$default_text"
  CONNECTOR_DEFINITIONS=$(grep "dockerRepository" -h -A1 airbyte-config/init/src/main/resources/seed/*.yaml | grep -v -- "^--$" | tr -d ' ')
  [ -z "CONNECTOR_DEFINITIONS" ] && echo "ERROR: Could not find any connector definition." && exit 1

  while IFS=":" read -r _ REPO; do
      IFS=":" read -r _ TAG
      printf "\t${REPO}: ${TAG}\n"
      if docker_tag_exists "$REPO" "$TAG"; then
          printf "\tSTATUS: found\n"
      else
          printf "\tERROR: not found!\n" && exit 1
      fi
  done <<< "${CONNECTOR_DEFINITIONS}"
  echo -e "$blue_text""Success! All connector images exist!""$default_text"
}

main() {
  assert_root

  SUBSET=${1:-all} # default to all.
  [[ ! "$SUBSET" =~ ^(all|platform|connectors)$ ]] && echo "Usage ./tools/bin/check_image_exists.sh [all|platform|connectors]" && exit 1
  echo -e "$blue_text""checking images for: $SUBSET""$default_text"

  [[ "$SUBSET" =~ ^(all|platform)$ ]] && checkPlatformImages
  [[ "$SUBSET" =~ ^(all|platform|connectors)$ ]] && checkNormalizationImages
  [[ "$SUBSET" =~ ^(all|connectors)$ ]] && checkConnectorImages
  echo -e "$blue_text""Image check complete.""$default_text"
  test -f header.txt     && rm header.txt
  test -f compose_output && rm compose_output
}

main "$@"
