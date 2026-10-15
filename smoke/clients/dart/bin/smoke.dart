// Smoke: the published zebridge package enrolls with an invite, seeds smoke_items, writes
// one row and waits for PostgreSQL's verdict, then prints "ready" and waits for the row
// run.sh inserts in PostgreSQL (it can only arrive by the live CDC tail). Exit 0 on both.
import 'dart:io';
import 'package:zebridge/zebridge.dart';

const name = 'dart';

Never fail(String msg) {
  print('✗ $name: $msg');
  exit(1);
}

Future<void> main() async {
  final env = Platform.environment;
  final zb = await ZeBridgeWorker.spawn({
    'bridgeUrl': env['ZB_BRIDGE_URL'],
    'invite': env['ZB_INVITE'],
    'identityPath': '/tmp/smoke-$name.identity',
    'dbPath': '/tmp/smoke-$name.db',
    'tables': ['smoke_items'],
    'heartbeatMs': 0,
  });
  final until = DateTime.now().add(const Duration(seconds: 30));
  while ((await zb.query("SELECT 1 AS one FROM smoke_items WHERE id = 'seed'")).isEmpty) {
    if (DateTime.now().isAfter(until)) fail('the seed row never arrived');
    await Future.delayed(const Duration(milliseconds: 500));
  }
  final verdict = zb.reports.where((r) => r.outcomes.isNotEmpty).first;
  const id = 'from-$name';
  await zb.mutate('smoke_items', 'insert', {'id': id},
      {'id': id, 'tenant_id': zb.tenants.first, 'note': 'written by $name',
       'updated_at': DateTime.now().toUtc().toIso8601String()});
  final r = await verdict.timeout(const Duration(seconds: 30), onTimeout: () => fail('no verdict in 30 s'));
  final outcome = r.outcomes.first['outcome'];
  if (outcome != 'applied') fail('verdict ${r.outcomes.first}');
  print('ready');
  final live = DateTime.now().add(const Duration(seconds: 30));
  while ((await zb.query('SELECT 1 AS one FROM smoke_items WHERE id = ?', ['live-$name'])).isEmpty) {
    if (DateTime.now().isAfter(live)) fail("PostgreSQL's live row never arrived (the CDC tail)");
    await Future.delayed(const Duration(milliseconds: 500));
  }
  print('✓ $name: enrolled, seeded, write applied, live change received (tenant ${zb.tenants.first})');
  await zb.close();
  exit(0);
}
