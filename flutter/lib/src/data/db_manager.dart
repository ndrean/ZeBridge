import 'package:sqflite/sqflite.dart';
import 'package:path/path.dart' as p;
import 'dart:convert';
import 'package:collection/collection.dart';

class DbManager {
  final String dbName;
  Database? _db;

  DbManager({required this.dbName});

  Future<void> init() async {
    final dbPath = await getDatabasesPath();
    final path = p.join(dbPath, dbName);
    _db = await openDatabase(
      path,
      version: 1,
      onCreate: (db, version) async {
        await db.execute('''
          CREATE TABLE IF NOT EXISTS _zebridge_sync (
            id INTEGER PRIMARY KEY,
            global_last_lsn INTEGER,
            global_last_seq INTEGER
          );
        ''');
        await db.insert('_zebridge_sync', {
          'id': 1,
          'global_last_lsn': 0,
          'global_last_seq': 0,
        }, conflictAlgorithm: ConflictAlgorithm.ignore);
      },
    );
  }

  Database get db => _db!;

  Future<Map<String, dynamic>> getSyncState() async {
    if (_db == null || !_db!.isOpen) return {'global_last_lsn': 0, 'global_last_seq': 0};
    try {
      final res = await db.query('_zebridge_sync', where: 'id = 1');
      if (res.isNotEmpty) {
        return res.first;
      }
    } catch (_) {}
    return {'global_last_lsn': 0, 'global_last_seq': 0};
  }

  Future<void> updateSyncState({int? lsn, int? seq}) async {
    if (_db == null || !_db!.isOpen) return;
    final map = <String, dynamic>{};
    if (lsn != null) map['global_last_lsn'] = lsn;
    if (seq != null) map['global_last_seq'] = seq;
    if (map.isNotEmpty) {
      await db.update('_zebridge_sync', map, where: 'id = 1');
    }
  }

  Future<void> dropTable(String table) async {
    if (_db == null || !_db!.isOpen) return;
    await db.execute('DROP VIEW IF EXISTS ${table}_view;');
    await db.execute('DROP TABLE IF EXISTS $table;');
  }

  Future<void> applySchema(String table, Map<String, dynamic> val, Map<String, dynamic>? existingTableState) async {
    if (_db == null || !_db!.isOpen) return;
    final sqlite = val['sqlite'];
    final pkCols = (sqlite['pk_columns'] as List<dynamic>?)?.cast<String>() ?? (sqlite['pk'] != null ? [sqlite['pk'] as String] : <String>[]);
    final cols = (sqlite['columns'] as List<dynamic>).cast<Map<String, dynamic>>();
    
    final names = cols.map((c) => c['name'] as String).toList();
    final inlinePk = pkCols.length == 1;
    
    bool isPk(String name) => pkCols.contains(name);
    
    String ddl(Map<String, dynamic> c) {
      final name = c['name'] as String;
      final type = c['type'] as String;
      return '"$name" $type' +
          (isPk(name) ? ' NOT NULL' : '') +
          (inlinePk && name == pkCols[0] ? ' PRIMARY KEY' : '');
    }
    
    final tableConstraint = pkCols.length > 1 ? ', PRIMARY KEY (${pkCols.map((c) => '"$c"').join(', ')})' : '';
    
    if (existingTableState == null) {
      final stmt = 'CREATE TABLE IF NOT EXISTS $table (${cols.map(ddl).join(', ')}$tableConstraint);';
      await db.execute(stmt);
      return;
    }
    
    final existingColumns = (existingTableState['columns'] as List<String>);
    final added = names.where((n) => !existingColumns.contains(n)).toList();
    final removed = existingColumns.where((n) => !names.contains(n)).toList();
    
    bool needsRebuild = false;
    for (var r in removed) {
      if (isPk(r)) {
        needsRebuild = true;
        break;
      }
    }
    
    if (!needsRebuild) {
      for (final add in added) {
        final colDef = cols.firstWhere((c) => c['name'] == add);
        await db.execute('ALTER TABLE $table ADD COLUMN ${ddl(colDef)};');
      }
      for (final rm in removed) {
         await db.execute('ALTER TABLE $table DROP COLUMN "$rm";');
      }
    } else {
      final tmp = '${table}__migrating';
      await db.execute('DROP TABLE IF EXISTS $tmp;');
      await db.execute('CREATE TABLE $tmp (${cols.map(ddl).join(', ')}$tableConstraint);');
      final common = names.where((n) => existingColumns.contains(n)).map((n) => '"$n"').toList();
      if (common.isNotEmpty) {
        final commonStr = common.join(', ');
        await db.execute('INSERT INTO $tmp ($commonStr) SELECT $commonStr FROM $table;');
      }
      await db.execute('DROP TABLE IF EXISTS $table;');
      await db.execute('ALTER TABLE $tmp RENAME TO $table;');
    }
  }

  Future<void> truncateTable(String table) async {
    if (_db == null || !_db!.isOpen) return;
    await db.execute('DELETE FROM $table');
  }

  Future<void> upsertRows(String table, List<Map<String, dynamic>> rows, List<String> pkCols) async {
    if (_db == null || !_db!.isOpen || rows.isEmpty) return;
    
    final batch = db.batch();
    for (final row in rows) {
      final columns = row.keys.map((k) => '"$k"').join(', ');
      final placeholders = row.keys.map((_) => '?').join(', ');
      final values = row.values.toList();
      
      final conflictCols = pkCols.map((k) => '"$k"').join(', ');
      final updateSet = row.keys
          .where((k) => !pkCols.contains(k))
          .map((k) => '"$k" = excluded."$k"')
          .join(', ');
          
      String sql = 'INSERT INTO $table ($columns) VALUES ($placeholders)';
      if (updateSet.isNotEmpty) {
        sql += ' ON CONFLICT($conflictCols) DO UPDATE SET $updateSet';
      } else if (pkCols.isNotEmpty) {
        sql += ' ON CONFLICT($conflictCols) DO NOTHING';
      }
      
      batch.execute(sql, values);
    }
    await batch.commit(noResult: true);
  }

  Future<void> deleteRow(String table, Map<String, dynamic> pkData) async {
    if (_db == null || !_db!.isOpen) return;
    final where = pkData.keys.map((k) => '"$k" = ?').join(' AND ');
    final values = pkData.values.toList();
    await db.delete(table, where: where, whereArgs: values);
  }
}
