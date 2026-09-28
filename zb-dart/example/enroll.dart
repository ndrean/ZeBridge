// Enroll from an invite, read, write, and see the verdict — the whole app side.
//   dart run example/enroll.dart <bridgeUrl> <identityPath> <dbPath> [invite]
import 'package:zebridge/zebridge.dart';

Future<void> main(List<String> args) async {
  final zb = await ZeBridgeWorker.spawn({
    'bridgeUrl': args[0],
    'identityPath': args[1],
    'dbPath': args[2],
    if (args.length > 3) 'invite': args[3],
    'tables': ['test_types'],
    'heartbeatMs': 0,
  });
  print('tenants ${zb.tenants}, unseeded ${zb.unseeded}');
  final n = (await zb.query('SELECT count(*) AS n FROM test_types WHERE deleted_at IS NULL')).first['n'];
  const text = 'écrit en Dart 🎯 — ok';
  final echo = (await zb.query('SELECT ? AS s', [text])).first['s'];
  print('live rows $n; utf-8 round trip ${echo == text ? 'ok' : echo}');
  if (args.length > 3) {
    final settled = zb.reports.where((r) => r.settled > 0).first;
    final now = DateTime.now().toUtc().toIso8601String();
    final uid = 'd${DateTime.now().microsecondsSinceEpoch}'.padRight(8, '0');
    final key = {'uid': '00000000-0000-4000-8000-${uid.substring(uid.length - 12)}'};
    await zb.mutate('test_types', 'insert', key, {...key, 'some_text': text, 'tenant_id': zb.tenants.first, 'inserted_at': now, 'updated_at': now});
    final r = await settled.timeout(const Duration(seconds: 10));
    print('verdict settled: ${r.settled}');
    await zb.mutate('test_types', 'delete', key);
  }
  await zb.close();
}
