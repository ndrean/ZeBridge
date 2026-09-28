/// ZeBridge for Dart and Flutter: `ZeBridgeWorker.spawn(options)` — one isolate owns the
/// libzb handle and its poll loop; the app talks to it with futures and a report stream.
/// The options are the ones every ZeBridge client reads (CLIENTS.md): the first run
/// needs only `bridgeUrl` and `invite`; the identity is kept next to the replica and the
/// JWT renews itself.
library;

export 'src/native.dart' show ZeBridge, ZeBridgeException, PollReport, zbAbi;
export 'src/worker.dart' show ZeBridgeWorker;
