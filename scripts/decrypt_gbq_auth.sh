#!/usr/bin/env bash

set -euo pipefail
IFS=$'\n\t'

PRIVATE_KEY=${TRAVIS_BUILD_DIR}/scripts/gbq-enc-private.key
ENC_KEY_FILE=${TRAVIS_BUILD_DIR}/scripts/keyfile.enc
DEC_KEY_FILE=${TRAVIS_BUILD_DIR}/scripts/keyfile.txt
ENC_AUTH_FILE=${TRAVIS_BUILD_DIR}/scripts/gbqAuthFile.enc
DEC_AUTH_FILE=$TRAVIS_BUILD_DIR/core/src/test/resources/gbqAuthFile.json

openssl rsautl -decrypt -inkey ${PRIVATE_KEY} -in ${ENC_KEY_FILE} -out ${DEC_KEY_FILE} -pass env:${GBQ_DECRYPT_PASSWORD}

openssl enc -d -aes-256-cbc -in ${ENC_AUTH_FILE} -out ${DEC_AUTH_FILE} -pass file:${DEC_KEY_FILE}


