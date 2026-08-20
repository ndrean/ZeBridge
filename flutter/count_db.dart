import 'package:sqflite_common_ffi/sqflite_ffi.dart';
import 'package:path/path.dart' as p;

void main() async {
  sqfliteFfiInit();
  final dbPath = await databaseFactoryFfi.getDatabasesPath();
  final db = await databaseFactoryFfi.openDatabase(p.join(dbPath, 'zebridge_client.db'));
  final res = await db.rawQuery('SELECT COUNT(*) FROM test_types');
  print('Total rows: \$res');
  final res2 = await db.rawQuery('SELECT COUNT(*) FROM test_types WHERE "deleted_at" IS NULL');
  print('Active rows: \$res2');
  final res3 = await db.rawQuery('SELECT uid, count(*) as c FROM test_types GROUP BY uid HAVING c > 1');
  print('Duplicates: \$res3');
}
