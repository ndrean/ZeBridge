// The poll report is the pull model's whole contract with the UI (README, "The C ABI
// library"): what changed, what was seeded, what settled, and — from the worker —
// why a poll failed. Parsed from the C card's JSON exactly as it comes.
import 'package:flutter_test/flutter_test.dart';
import 'package:zebridge_flutter/src/data/zebridge.dart';

void main() {
  test('a poll report carries the changed and seeded tables', () {
    final r = PollReport.fromJson({
      'applied': 2,
      'settled': 1,
      'changed_tables': ['app_users', 'app_orders'],
      'seeded': <String>[],
    });
    expect(r.applied, 2);
    expect(r.settled, 1);
    expect(r.changedTables, ['app_users', 'app_orders']);
    expect(r.seeded, isEmpty);
    expect(r.error, isNull);
  });

  test('a report of a failed poll names the reason and changes nothing', () {
    final r = PollReport.fromJson({
      'applied': 0,
      'settled': 0,
      'changed_tables': <String>[],
      'seeded': <String>[],
      'error': 'Revoked',
    });
    expect(r.error, 'Revoked');
    expect(r.changedTables, isEmpty);
  });

  test('missing lists read as empty (an older library)', () {
    final r = PollReport.fromJson({'applied': 0, 'settled': 0});
    expect(r.changedTables, isEmpty);
    expect(r.seeded, isEmpty);
  });
}
