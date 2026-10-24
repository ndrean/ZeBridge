#!/bin/sh
# The files the zebridge image carries besides the .deb, into a Docker build context:
#
#   deploy/image-context.sh <ctx>     (release.yml; a local build of the image too)
#
#   <ctx>/quickstart/   docker-compose.quickstart.yml's one-time setup
#   <ctx>/compose/      deploy/compose's configuration: HAProxy, Alloy, the scripts.
#                       compose.yml and .env stay out: they are what the operator downloads
#                       and fills in.
set -eu
ctx=$1
root=$(cd "$(dirname "$0")/.." && pwd)
mkdir -p "$ctx/quickstart" "$ctx/compose"
cp "$root/quickstart/init.sh" "$root/quickstart/survey.sql" "$root/examples/05-tables/web-consumer/demo.sql" "$ctx/quickstart/"
cp -R "$root/deploy/compose/haproxy.cfg" "$root/deploy/compose/alloy" "$root/deploy/compose/scripts" "$ctx/compose/"
