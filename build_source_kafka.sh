#!/usr/bin/env bash

set -e

SHA=$(git rev-parse --short HEAD)
ARCH="amd64"
TAG="dev-${ARCH}-${SHA}"

echo $TAG;

# and then this flag in the airbyte-ci command / build subcommand --use-host-gradle-dist-tar && \

./gradlew :airbyte-integrations:connectors:source-kafka:build
#  $ build  --use-host-gradle-dist-tar && \

airbyte-ci connectors --name=source-kafka build -a "linux/${ARCH}" --tag "${TAG}" --use-host-gradle-dist-tar

docker tag airbyte/source-kafka:${TAG} vpipkt/source-kafka:${TAG}

docker push vpipkt/source-kafka:${TAG}

echo "done"