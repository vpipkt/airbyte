
SHA=$(git rev-parse --short HEAD)
ARCH="amd64"
TAG="dev-${ARCH}-${SHA}"

echo $TAG;

./gradlew :airbyte-integrations:connectors:source-kafka:build && \
  airbyte-ci connectors --name=source-kafka build -a "linux/${ARCH}" --tag "${TAG}" --use-host-gradle-dist-tar && \
  docker tag airbyte/source-kafka:${TAG} vpipkt/source-kafka:${TAG} && \
  docker push vpipkt/source-kafka:${TAG}
