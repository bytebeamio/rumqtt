#!/usr/bin/env sh

# Fail on error
set -o errexit
# Disable wildcard character expansion
set -o noglob
# Disable uninitialized variable usage
set -o nounset
# Disable error masking in pipe
# shellcheck disable=SC3040
if (set -o pipefail 2> /dev/null); then
    set -o pipefail
fi

if [ $# -ne 1 ]; then
  echo "Pass number of bikes"
  echo "Usage: ./createvehicle 10"
  exit
fi

for i in $(seq 1 "$1"); do
  bike_id=bike-$i
  wget https://pki.google.com/roots.pem

  openssl req -x509 -newkey rsa:2048 -keyout rsa_private.pem -nodes -out rsa_cert.pem -subj "/CN=unused"
  openssl rsa -in rsa_private.pem -outform DER -out rsa_private.der
  # openssl x509 -in roots.pem -outform DER -out roots.der

  gcloud iot devices delete -q "$bike_id" --region=asia-east1 --registry=iotcore || true
  gcloud iot devices create "$bike_id" --region=asia-east1 --registry=iotcore --public-key path=rsa_cert.pem,type=rs256

  mkdir -p "$bike_id"
  mv roots.pem rsa_private.pem rsa_private.der "$bike_id"
  rm -rf ./*.pem ./*.der
done
