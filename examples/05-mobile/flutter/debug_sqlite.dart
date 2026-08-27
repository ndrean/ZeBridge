import 'dart:io';
import 'package:sqlite3/sqlite3.dart';

void main() {
  final dbPath = '/Users/nevendrean/Library/Containers/com.example.zebridgeFlutter/Data/Documents/zebridge_client.db';
  if (!File(dbPath).existsSync()) {
    print('DB not found at \$dbPath');
    return;
  }
  final db = sqlite3.open(dbPath);
  final res = db.select('SELECT COUNT(*) as c FROM test_types');
  print('Total rows: \${res.first['c']}');
  final res2 = db.select('SELECT COUNT(*) as c FROM test_types WHERE deleted_at IS NULL');
  print('Active rows: \${res2.first['c']}');
  final res3 = db.select('SELECT uid, COUNT(*) as c FROM test_types GROUP BY uid HAVING c > 1');
  print('Duplicates: \$res3');
  final res4 = db.select('PRAGMA table_info(test_types)');
  print('Table info: \$res4');
  db.dispose();
}
