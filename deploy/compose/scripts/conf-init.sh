#!/bin/sh
# The configuration the other services read (HAProxy, Prometheus, Grafana, the scripts),
# copied from this image into the zb-conf volume at every start: it follows ZB_VERSION.
set -eu
rm -rf /conf/* 2>/dev/null || true
cp -R /usr/share/zebridge/compose/. /conf/
chmod -R a+rX /conf
echo "configuration from release $(bridge --version | cut -d' ' -f2) in place"
