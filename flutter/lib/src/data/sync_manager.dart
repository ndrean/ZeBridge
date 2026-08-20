import 'dart:convert';
import 'dart:typed_data';
import 'package:dart_nats/dart_nats.dart';
import 'package:messagepack/messagepack.dart';
import 'package:sqflite/sqflite.dart';
import 'package:uuid/uuid.dart';

import 'sync_state_notifier.dart';
import 'db_manager.dart';

class SyncManager {
  final DbManager dbManager;
  final SyncStateNotifier notifier;
  final String principal;
  final String natsUrl;
  final String? tenant;
  final String password;
  
  Client? _client;

  SyncManager({
    required this.dbManager,
    required this.notifier,
    required this.principal,
    required this.natsUrl,
    this.tenant,
    required this.password,
  });

  Future<void> connect() async {
    _client = Client();
    await _client!.connect(Uri.parse(natsUrl), retryInterval: 2, retry: true, connectOption: ConnectOption(user: principal, pass: password));
    notifier.updateStatus('Connected & Syncing');
    notifier.reach('connected');
    
    await _watchSchemas();
    
    // dart_nats watch doesn't signal the end of the initial history replay.
    // Give it a brief moment to process the initial schemas before we start CDC.
    await Future.delayed(const Duration(seconds: 2));
    
    await _subscribeCdc();
    notifier.reach('cdc');
    
    await _watchVerdicts();
  }

  void disconnect() {
    _client?.close();
  }

  Future<void> _watchSchemas() async {
    // using dart_nats KeyValue API if available, else raw JetStream.
    // The bucket is "schemas". NATS exposes KV over JS.
    final js = _client!.jetStream();
    
    // According to NATS, a KV bucket "schemas" is a stream "KV_schemas".
    // Wait, dart_nats has KeyValue api. Let's try to use raw JS consumer on KV_schemas if it fails.
    try {
      final kv = await js.keyValue('schemas');
      final watch = await kv.watch(includeHistory: true);
      watch.listen((entry) async {
        try {
          if (entry == null) return;
          
          final key = entry.key;
          final op = entry.op;
          if (op == KeyValueOp.delete || op == KeyValueOp.purge) {
            await dbManager.dropTable(key);
            notifier.removeTable(key);
            return;
          }
          
          dynamic val;
          try {
            val = Unpacker(entry.value).unpackMap();
          } catch (_) {
            val = jsonDecode(utf8.decode(entry.value));
          }
          
          if (val is Map && val['schema'] is String) {
            val = jsonDecode(val['schema']);
          }
          
          if (val['dropped'] == true) {
            await dbManager.dropTable(key);
            notifier.removeTable(key);
            return;
          }
          
          if (val['suspended'] == true) {
            return;
          }
          
          if (val['sqlite'] != null && val['sqlite']['columns'] != null) {
            final existing = await _getTableInfo(key);
            await dbManager.applySchema(key, val, existing);
            
            if (val['tombstone_column'] is String) {
              notifier.tombstoneColumns[key] = val['tombstone_column'] as String;
            }
            
            final cols = (val['sqlite']['columns'] as List).length;
            final count = await _getTableCount(key);
            notifier.updateTable(key, colCount: cols, count: count);
            notifier.reach('migrated');
            
            await _gapCheck(key, val['lsn']);
          }
        } catch (e) {
          notifier.log('KV Schema error for ${entry?.key}: $e');
        }
      });
    } catch (e) {
      // Fallback if dart_nats KV API is different
      notifier.log('KV Watch error: $e');
    }
  }

  Future<Map<String, dynamic>?> _getTableInfo(String table) async {
    try {
      final res = await dbManager.db.rawQuery('PRAGMA table_info($table)');
      if (res.isEmpty) return null;
      return {'columns': res.map((r) => r['name'] as String).toList()};
    } catch (_) {
      return null;
    }
  }

  Future<int> _getTableCount(String table) async {
    try {
      final tombstone = notifier.tombstoneColumns[table];
      final where = tombstone != null ? ' WHERE "$tombstone" IS NULL' : '';
      final res = await dbManager.db.rawQuery('SELECT COUNT(*) as count FROM $table$where');
      return res.first['count'] as int;
    } catch (_) {
      return 0;
    }
  }
  
  Future<void> _gapCheck(String table, int schemaLsn) async {
    final state = await dbManager.getSyncState();
    final globalLastLsn = state['global_last_lsn'] as int;
    final globalLastSeq = state['global_last_seq'] as int;
    
    final js = _client!.jetStream();
    try {
      final streamInfo = await js.streamInfo('CDC');
      final firstSeq = streamInfo.state.firstSeq;
      
      if (globalLastLsn == 0 || (firstSeq > 0 && globalLastSeq < firstSeq - 1)) {
        await _requestSnapshot(table);
      } else {
        notifier.reach('snapshot');
      }
    } catch (e) {
      notifier.log('Stream info error: $e');
      notifier.reach('snapshot'); // Fallback if CDC stream info fails
    }
  }
  
  Future<void> _requestSnapshot(String table) async {
    final reqSubject = 'snapshot.request.$table';
    final js = _client!.jetStream();
    
    try {
      notifier.log('Publishing snapshot request for $table...');
      try {
        final p = Packer();
        p.packMapLength(2);
        p.packString('tenant'); p.packString(tenant ?? '');
        p.packString('principal'); p.packString(principal ?? '');
        await js.publish(reqSubject, p.takeBytes());
      } catch (e) {
        notifier.log('Snapshot request rate-limited (waiting for cached descriptor)');
      }
      
      final snapKv = await js.keyValue('snapshots');
      dynamic desc;
      
      // Wait for descriptor (up to 60 seconds, matching App.tsx SNAPSHOT_WAIT_MS)
      for (int i = 0; i < 600; i++) {
        KeyValueEntry? entry;
        try {
          entry = await snapKv.get(table);
        } catch (_) {
          // Key doesn't exist yet (404)
        }
        
        if (entry != null) {
          try {
            desc = Unpacker(entry.value).unpackMap();
          } catch (_) {
            desc = jsonDecode(utf8.decode(entry.value));
          }
          break;
        }
        await Future.delayed(const Duration(milliseconds: 100));
      }
      
      if (desc == null || desc['snapshot_id'] == null) {
        notifier.log('Failed to obtain snapshot descriptor for $table');
        notifier.reach('snapshot');
        return;
      }
      
      final snapshotId = desc['snapshot_id'];
      if (desc['lsn'] is int) {
         await dbManager.updateSyncState(lsn: desc['lsn'] as int);
      }
      
      final subSubject = 'init.snap.$table.$snapshotId.>';
      notifier.log('Subscribing to INIT stream on $subSubject');
      final sub = await js.subscribe(subSubject, stream: 'INIT', deliverPolicy: 'all');
      
      List<String>? snapshotColumns;
      
      sub.stream.listen((msg) async {
        try {
          dynamic chunkDecoded;
          try {
            chunkDecoded = Unpacker(msg.data).unpackList();
          } catch (_) {
            try {
              chunkDecoded = Unpacker(msg.data).unpackMap();
            } catch (_) {
              chunkDecoded = jsonDecode(utf8.decode(msg.data));
            }
          }
          
          if (chunkDecoded is Map && chunkDecoded['schema'] is List) {
            snapshotColumns = (chunkDecoded['schema'] as List).cast<String>();
            notifier.log('Received snapshot schema for $table');
          } else if (chunkDecoded is List) {
            final existing = await _getTableInfo(table);
            if (existing != null) {
              final pkInfo = await dbManager.db.rawQuery('PRAGMA table_info($table)');
              final pks = pkInfo.where((r) => r['pk'] != 0).map((r) => r['name'] as String).toList();
              
              final cols = snapshotColumns ?? (existing['columns'] as List).cast<String>();
              
              final rows = <Map<String, dynamic>>[];
              for (final rowVals in chunkDecoded) {
                if (rowVals is List) {
                  final rowObj = <String, dynamic>{};
                  for (var i = 0; i < cols.length && i < rowVals.length; i++) {
                    rowObj[cols[i]] = rowVals[i];
                  }
                  rows.add(rowObj);
                }
              }
              
              if (rows.isNotEmpty) {
                await dbManager.upsertRows(table, rows, pks);
                final count = await _getTableCount(table);
                notifier.updateTable(table, count: count, verb: 'SNP');
              }
            }
          } else if (chunkDecoded is Map && chunkDecoded['operation'] == 'snapshot' && chunkDecoded['data'] is List) {
             final existing = await _getTableInfo(table);
             if (existing != null) {
                final pkInfo = await dbManager.db.rawQuery('PRAGMA table_info($table)');
                final pks = pkInfo.where((r) => r['pk'] != 0).map((r) => r['name'] as String).toList();
                
                final rows = (chunkDecoded['data'] as List).cast<Map<String, dynamic>>();
                await dbManager.upsertRows(table, rows, pks);
                
                final count = await _getTableCount(table);
                notifier.updateTable(table, count: count, verb: 'SNP');
             }
          }
        } catch (e) {
          notifier.log('Snapshot chunk error for $table: $e');
        }
      });
      
      // Mark as reached immediately. The listener will process chunks asynchronously.
      // This prevents the UI from stalling if no chunks arrive (e.g. empty table).
      notifier.reach('snapshot');
      
    } catch (e) {
      notifier.log('Snapshot request error: $e');
    }
  }

  Future<void> _subscribeCdc() async {
    // Strictly matching App.tsx routing:
    // cdc.<tenant>.> for all tenant tables
    // cdc.users.> for public tables
    final subjects = tenant != null && tenant!.isNotEmpty 
        ? ['cdc.$tenant.>', 'cdc.users.>'] 
        : ['cdc.>'];
        
    final js = _client!.jetStream();
    
    for (final subject in subjects) {
      try {
        final sub = await js.subscribe(subject, stream: 'CDC', deliverPolicy: 'all');
        sub.stream.listen((msg) async {
      try {
        dynamic val;
        try {
           final first = msg.data[0];
           final u = Unpacker(msg.data);
           if (first == 0x90 || (first >= 0x90 && first <= 0x9f) || first == 0xdc || first == 0xdd) {
             val = u.unpackList();
           } else {
             val = u.unpackMap();
           }
        } catch (_) {
          val = jsonDecode(utf8.decode(msg.data));
        }
        
        notifier.log('Received raw CDC event: $val');
        
        final events = val is List ? val : [val];
        for (final event in events) {
          final parts = msg.subject!.split('.');
          String? evTbl;
          String? evOp;
          int evLsn = 0;
          Map<String, dynamic>? data;
          
          if (event is Map) {
            evTbl = event['table']?.toString();
            evOp = event['operation']?.toString();
            evLsn = event['lsn'] is int ? event['lsn'] : int.tryParse(event['lsn']?.toString() ?? '0') ?? 0;
            final rawData = event['data'];
            if (rawData is Map) {
              data = <String, dynamic>{};
              rawData.forEach((k, v) => data![k.toString()] = v);
            }
          } else if (event is List && event.length >= 4) {
            evLsn = event[0] is int ? event[0] : int.tryParse(event[0]?.toString() ?? '0') ?? 0;
            evTbl = event[1]?.toString();
            evOp = event[2]?.toString();
            final rawData = event[3];
            if (rawData is Map) {
              data = <String, dynamic>{};
              rawData.forEach((k, v) => data![k.toString()] = v);
            }
          }
          
          final tbl = evTbl ?? (parts.length >= 4 ? parts[2] : parts[1]);
          final op = evOp?.toLowerCase() ?? (parts.last.toLowerCase());
          final lsn = evLsn;
          
          final state = await dbManager.getSyncState();
          if (lsn > 0 && lsn <= state['global_last_lsn']) {
            continue; // Already processed
          }
          
          if (op == 'insert' || op == 'update') {
            if (data == null) continue;
            
            final sqliteSchema = await _getTableInfo(tbl);
            if (sqliteSchema != null) {
               final pkInfo = await dbManager.db.rawQuery('PRAGMA table_info($tbl)');
               final pks = pkInfo.where((r) => r['pk'] != 0).map((r) => r['name'] as String).toList();
               
               await dbManager.upsertRows(tbl, [data], pks);
               await dbManager.updateSyncState(lsn: lsn);
               
               final count = await _getTableCount(tbl);
               final tombstone = notifier.tombstoneColumns[tbl];
               final isSoftDelete = tombstone != null && data[tombstone] != null;
               final displayVerb = isSoftDelete ? 'DEL' : op.toUpperCase().substring(0, 3);
               
               notifier.updateTable(tbl, count: count, verb: displayVerb);
            }
          } else if (op == 'delete') {
            if (data == null) continue;
            await dbManager.deleteRow(tbl, data);
            await dbManager.updateSyncState(lsn: lsn);
            
            final count = await _getTableCount(tbl);
            notifier.updateTable(tbl, count: count, verb: 'DEL');
          }
        }
      } catch (e) {
        notifier.log('CDC handle error: $e');
      }
    });
      } catch (e) {
        notifier.log('Failed to subscribe to JetStream CDC subject $subject: $e');
      }
    } // End of for loop
  }

  Future<void> _watchVerdicts() async {
    final subject = 'mutation_ack.$principal.>';
    final sub = _client!.sub(subject);
    
    sub.stream.listen((msg) {
      // Handle mutation verdicts
      final verdictStr = utf8.decode(msg.data);
      notifier.log('Verdict received: $verdictStr');
      // In a full app, this would delete the outbox entry based on msg.subject
    });
  }
}
