# JWT renewal on a phone (by hand)

libzb on an iPhone enrolls with an invite, follows `counter_public`, and renews its JWT
by itself. The screen shows the phone's clock, its estimate of the bridge's clock, the
JWT's times and every renewal. Move the phone's clock and watch (NOTES §10kr).

The bridge, with short JWTs and listening on the LAN, enrollment armed:

    ENROLL_JWT_TTL_SECONDS=300 BRIDGE_BIND=0.0.0.0 ZB_SIGNING_SEED=… ZB_ACCOUNT_PUB=… \
      bridge --pub my_pub --slot my_slot --port 27434

An invite (16+ characters):

    INSERT INTO zebridge_invites (code, principal, tenant_id) VALUES ('phone-renew-…', 'phone_renew', 'acme');

Build and install (libzb comes with the zebridge package; in this repository, run
`zb-dart/scripts/build-prebuilt.sh` once):

    flutter pub get
    flutter build ios --release --dart-define=ZB_BRIDGE_URL=http://192.168.1.11:27434 \
      --dart-define=ZB_NATS_URL=nats://192.168.1.11:4222 --dart-define=ZB_INVITE=<code>
    xcrun devicectl device install app --device <udid> build/ios/iphoneos/Runner.app

The invite is used once; later launches open on the stored identity. A long press on the bin
deletes the replica and the identity (a new invite is then needed).
