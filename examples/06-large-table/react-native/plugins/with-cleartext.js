// Dev LAN only: the app speaks ws:// (NATS WebSocket) and nats:// to a Mac on the LAN.
// Android release builds refuse cleartext by default ("CLEARTEXT communication not
// permitted"); only the debug manifest allows it. A deployed app uses wss:// and drops this.
const { withAndroidManifest } = require('expo/config-plugins');

module.exports = (config) =>
  withAndroidManifest(config, (c) => {
    c.modResults.manifest.application[0].$['android:usesCleartextTraffic'] = 'true';
    return c;
  });
