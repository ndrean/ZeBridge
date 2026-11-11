import 'dart:io';
import 'package:zebridge/zebridge.dart';

Future<void> main(List<String> args) async {
  final tmp = await Directory.systemTemp.createTemp('zb-dart-ordering-');
  try {
    final client = await ZeBridgeWorker.spawn({
      'natsUrl': args.single, 'tables': ['wr_rows'], 'principal': 'tester',
      'clientId': 'dart', 'dbPath': '${tmp.path}/replica.sqlite3',
    });
    try {
      Future<String> write(String uid, {String? after, bool independent = false}) async {
        final key = {'uid': uid};
        final result = after == null && !independent
            ? await client.mutate('wr_rows', 'INSERT', key, key)
            : await client.mutateWith('wr_rows', 'INSERT', key,
                values: key, after: after, independent: independent);
        return result['msgId'] as String;
      }

      final parent = await write('parent');
      await write('unrelated');
      final child = await write('child', after: parent);
      await write('independent', independent: true);
      await write('tail');
      final rows = await client.query(
          'SELECT ordered, predecessor FROM _zebridge_outbox ORDER BY created_at');
      if (rows.length != 5 || rows[2]['predecessor'] != parent ||
          rows[3]['ordered'] != 0 || rows[3]['predecessor'] != null ||
          rows[4]['predecessor'] != child) {
        throw StateError('Unexpected outbox: $rows');
      }
    } finally {
      await client.close();
    }
  } finally {
    await tmp.delete(recursive: true);
  }
}
