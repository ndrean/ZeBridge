import 'package:flutter/material.dart';
import 'package:sqflite/sqflite.dart';
import 'package:path/path.dart' as p;
import '../data/db_manager.dart';
import '../data/sync_manager.dart';
import '../data/sync_state_notifier.dart';

class ZeBridgeApp extends StatelessWidget {
  const ZeBridgeApp({Key? key}) : super(key: key);

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'ZeBridge Flutter',
      theme: ThemeData(
        colorScheme: ColorScheme.fromSeed(
          seedColor: Colors.deepPurple,
          brightness: Brightness.light,
        ),
        useMaterial3: true,
      ),
      home: const HomeScreen(),
    );
  }
}

class HomeScreen extends StatefulWidget {
  const HomeScreen({Key? key}) : super(key: key);

  @override
  State<HomeScreen> createState() => _HomeScreenState();
}

class _HomeScreenState extends State<HomeScreen> {
  late DbManager dbManager;
  late SyncManager syncManager;
  late SyncStateNotifier notifier;

  @override
  void initState() {
    super.initState();
    notifier = SyncStateNotifier();
    _initApp();
  }

  Future<void> _initApp() async {
    notifier.updateStatus('Initializing DB...');
    dbManager = DbManager(dbName: 'zebridge_client.db');
    await dbManager.init();

    notifier.updateStatus('Connecting to NATS...');
    syncManager = SyncManager(
      dbManager: dbManager,
      notifier: notifier,
      principal: 'alice',
      tenant: 'acme',
      natsUrl: 'ws://localhost:8080',
      password: 's3cret',
    );
    
    try {
      await syncManager.connect();
    } catch (e) {
      notifier.updateStatus('Error: $e');
    }
  }

  Widget _buildPhaseStrip() {
    return ListenableBuilder(
      listenable: notifier,
      builder: (context, _) {
        return Row(
          mainAxisAlignment: MainAxisAlignment.center,
          children: [
            _PhaseBadge('connected', notifier.isConnected),
            const SizedBox(width: 8),
            _PhaseBadge('migrated', notifier.isMigrated),
            const SizedBox(width: 8),
            _PhaseBadge('snapshot', notifier.isSnapshot),
            const SizedBox(width: 8),
            _PhaseBadge('cdc', notifier.isCdc),
          ],
        );
      }
    );
  }

  Widget _buildTableGrid() {
    return ListenableBuilder(
      listenable: notifier,
      builder: (context, _) {
        final tables = notifier.tableCounts.keys.toList()..sort();
        if (tables.isEmpty) {
          return const Center(child: Text('No tables syncing yet.', style: TextStyle(color: Colors.grey)));
        }

        return ListView.builder(
          shrinkWrap: true,
          itemCount: tables.length,
          itemBuilder: (context, index) {
            final table = tables[index];
            final count = notifier.tableCounts[table] ?? 0;
            final cols = notifier.tableColumns[table] ?? 0;
            final verb = notifier.lastVerbs[table] ?? '—';
            
            Color verbColor = Colors.grey;
            if (verb == 'INS') verbColor = Colors.green;
            if (verb == 'UPD') verbColor = Colors.orange;
            if (verb == 'DEL') verbColor = Colors.red;

            return Card(
              margin: const EdgeInsets.symmetric(horizontal: 16, vertical: 4),
              child: ListTile(
                title: Text(table, style: const TextStyle(fontWeight: FontWeight.bold)),
                subtitle: Text('Schema: $cols columns'),
                trailing: Row(
                  mainAxisSize: MainAxisSize.min,
                  children: [
                    Container(
                      padding: const EdgeInsets.symmetric(horizontal: 8, vertical: 4),
                      decoration: BoxDecoration(
                        color: verbColor.withOpacity(0.2),
                        borderRadius: BorderRadius.circular(4),
                      ),
                      child: Text(verb, style: TextStyle(color: verbColor, fontWeight: FontWeight.bold, fontSize: 12)),
                    ),
                    const SizedBox(width: 16),
                    Text('$count rows', style: const TextStyle(fontSize: 16)),
                  ],
                ),
              ),
            );
          },
        );
      }
    );
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: const Text('ZeBridge Consumer'),
        centerTitle: true,
      ),
      body: SingleChildScrollView(
        child: Column(
          children: [
            const SizedBox(height: 16),
            ListenableBuilder(
              listenable: notifier,
              builder: (context, _) => Text(
                'Status: ${notifier.status}', 
                style: const TextStyle(fontSize: 18, fontWeight: FontWeight.bold)
              ),
            ),
            const SizedBox(height: 16),
            const Text('principal: alice  •  tenant: acme', style: TextStyle(color: Colors.grey)),
            const SizedBox(height: 24),
            _buildPhaseStrip(),
            const SizedBox(height: 32),
            const Text('Synced Tables', style: TextStyle(fontSize: 20, fontWeight: FontWeight.bold)),
            const SizedBox(height: 16),
            _buildTableGrid(),
            const SizedBox(height: 32),
            Row(
              mainAxisAlignment: MainAxisAlignment.center,
              children: [
                ElevatedButton(
                  onPressed: () async {
                    final state = await dbManager.getSyncState();
                    if (mounted) {
                       ScaffoldMessenger.of(context).showSnackBar(SnackBar(content: Text('Global LSN: ${state['global_last_lsn']} | Global Seq: ${state['global_last_seq']}')));
                    }
                  },
                  child: const Text('Check Sync State'),
                ),
                const SizedBox(width: 16),
                OutlinedButton(
                  style: OutlinedButton.styleFrom(foregroundColor: Colors.red),
                  onPressed: () async {
                    notifier.log('Wiping database...');
                    
                    // Shut down old connections
                    try { syncManager.disconnect(); } catch (_) {}
                    await dbManager.db.close();
                    
                    // Delete DB file
                    final dbPath = await getDatabasesPath();
                    final path = p.join(dbPath, dbManager.dbName);
                    await databaseFactory.deleteDatabase(path);
                    
                    // Reset UI
                    notifier.tableCounts.clear();
                    notifier.tableColumns.clear();
                    notifier.tombstoneColumns.clear();
                    notifier.lastVerbs.clear();
                    notifier.logs.clear();
                    notifier.updateStatus('Database wiped. Reconnecting...');
                    
                    // Re-run the full boot sequence!
                    _initApp();
                  },
                  child: const Text('Wipe Database'),
                ),
              ],
            ),
            const SizedBox(height: 32),
            const Text('Live Logs', style: TextStyle(fontSize: 18, fontWeight: FontWeight.bold)),
            const SizedBox(height: 8),
            Container(
              height: 200,
              margin: const EdgeInsets.symmetric(horizontal: 16, vertical: 8),
              decoration: BoxDecoration(
                color: Colors.black87,
                borderRadius: BorderRadius.circular(8),
              ),
              child: ListenableBuilder(
                listenable: notifier,
                builder: (context, _) {
                  if (notifier.logs.isEmpty) {
                    return const Center(child: Text('No logs yet...', style: TextStyle(color: Colors.grey)));
                  }
                  return ListView.builder(
                    padding: const EdgeInsets.all(8),
                    itemCount: notifier.logs.length,
                    itemBuilder: (context, index) {
                      return Padding(
                        padding: const EdgeInsets.symmetric(vertical: 2),
                        child: Text(
                          notifier.logs[index],
                          style: const TextStyle(fontFamily: 'monospace', color: Colors.greenAccent, fontSize: 12),
                        ),
                      );
                    },
                  );
                }
              ),
            ),
            const SizedBox(height: 32),
          ],
        ),
      ),
    );
  }
}

class _PhaseBadge extends StatelessWidget {
  final String label;
  final bool active;
  
  const _PhaseBadge(this.label, this.active, {Key? key}) : super(key: key);

  @override
  Widget build(BuildContext context) {
    return Container(
      padding: const EdgeInsets.symmetric(horizontal: 12, vertical: 6),
      decoration: BoxDecoration(
        color: active ? Colors.green : Colors.grey.shade300,
        borderRadius: BorderRadius.circular(16),
      ),
      child: Text(
        label,
        style: TextStyle(
          color: active ? Colors.white : Colors.grey.shade600,
          fontWeight: FontWeight.bold,
          fontSize: 12,
        ),
      ),
    );
  }
}
