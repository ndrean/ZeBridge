import 'dart:async';
import 'dart:io';
import 'package:flutter/material.dart';
import '../data/zebridge_worker.dart';

class ZeBridgeApp extends StatelessWidget {
  const ZeBridgeApp({Key? key}) : super(key: key);

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'ZeBridge Consumer',
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

class _HomeScreenState extends State<HomeScreen> with WidgetsBindingObserver {
  // The worker owns the libzb handle and the poll loop (zebridge_worker.dart);
  // this screen only sends it commands and listens to its reports.
  ZeBridgeWorker? zb;
  StreamSubscription? reportsSub;
  bool isConnected = false;
  String tenant = '—';
  String? lastError;

  Map<String, dynamic> counterPublic = {};
  Map<String, dynamic> counterTenant = {};
  List<dynamic> users = [];
  List<dynamic> orders = [];

  String sqlText =
      "SELECT count, note, updated_at, last_writer FROM app_orders ORDER BY updated_at DESC LIMIT 3";
  List<dynamic> sqlRows = [];
  String? sqlError;
  bool sqlLive = true;

  String formUserName = '';
  String formItem = '';
  String formCount = '1';
  String formNote = '';

  @override
  void initState() {
    super.initState();
    WidgetsBinding.instance.addObserver(this);
    _initApp();
  }

  /// The app going to the background pauses the loop — nothing touches the broker —
  /// and coming back resumes it: the next poll catches up on what was missed, the
  /// next flush sends what was written meanwhile. The outbox makes the pause harmless.
  @override
  void didChangeAppLifecycleState(AppLifecycleState state) {
    if (zb == null) return;
    switch (state) {
      case AppLifecycleState.resumed:
        zb!.resume();
        break;
      case AppLifecycleState.paused:
      case AppLifecycleState.inactive:
      case AppLifecycleState.hidden:
      case AppLifecycleState.detached:
        zb!.pause();
        break;
    }
  }

  Future<void> _initApp() async {
    try {
      // libzb speaks plain NATS over TCP (no websocket — that is the browser's
      // transport), and the operator-mode broker takes a creds file, nothing else.
      // Dev copy: the file straight from the repository, like the library path.
      final worker = await ZeBridgeWorker.spawn({
        "url": "nats://127.0.0.1:4222",
        "credsPath":
            "/Users/nevendrean/code/zig/ZeBridge/scripts/native/creds/alice.creds",
        // a writable place: a GUI app's working directory is not one
        "dbPath": "${Directory.systemTemp.path}/zb-flutter-alice.sqlite3",
        "principal": "alice",
        "tables": ["counter_public", "counter_tenant", "app_users", "app_orders"],
        "clientId": "flutter-client"
      });
      if (!mounted) {
        await worker.close();
        return;
      }
      zb = worker;
      setState(() {
        tenant = worker.tenant;
        isConnected = true;
      });
      await _refreshAll();
      // One report per poll that changed something: re-read only those tables.
      reportsSub = worker.reports.listen((report) {
        if (report.error != null) {
          setState(() => lastError = report.error);
          return;
        }
        _refreshAll(changedTables: [...report.changedTables, ...report.seeded]);
      });
    } catch (e) {
      setState(() => lastError = e.toString());
    }
  }

  Future<void> _refreshAll({List<String>? changedTables}) async {
    final w = zb;
    if (w == null) return;
    bool touched(String t) => changedTables == null || changedTables.contains(t);

    Map<String, dynamic>? nextPublic, nextTenant;
    List<dynamic>? nextUsers, nextOrders;

    if (touched('counter_public')) {
      try {
        final res = await w.query(
            "SELECT value, updated_at, last_writer FROM counter_public WHERE uid = '00000000-0000-4000-8000-00000000c0de'");
        if (res.isNotEmpty) nextPublic = res[0];
      } catch (_) {}
    }
    if (touched('counter_tenant')) {
      try {
        final res = await w.query(
            "SELECT uid, value, updated_at, last_writer FROM counter_tenant LIMIT 1");
        if (res.isNotEmpty) nextTenant = res[0];
      } catch (_) {}
    }
    if (touched('app_users')) {
      try {
        nextUsers = await w.query(
            "SELECT uid, name FROM app_users WHERE deleted_at IS NULL ORDER BY name");
      } catch (_) {}
    }
    if (touched('app_orders')) {
      try {
        nextOrders = await w.query(
            "SELECT * FROM app_orders WHERE deleted_at IS NULL ORDER BY updated_at DESC");
      } catch (_) {}
    }
    if (!mounted) return;
    setState(() {
      if (nextPublic != null) counterPublic = nextPublic;
      if (nextTenant != null) counterTenant = nextTenant;
      if (nextUsers != null) users = nextUsers;
      if (nextOrders != null) orders = nextOrders;
    });
    if (sqlLive &&
        (changedTables == null || changedTables.any((t) => sqlText.contains(t)))) {
      await _runSql();
    }
  }

  Future<void> _runSql() async {
    final w = zb;
    if (w == null || sqlText.trim().isEmpty) return;
    try {
      final res = await w.query(sqlText);
      if (!mounted) return;
      setState(() {
        sqlRows = res;
        sqlError = null;
      });
    } catch (e) {
      if (!mounted) return;
      setState(() {
        sqlError = e.toString();
        sqlRows = [];
      });
    }
  }

  Future<void> _bumpPublicCounter(int delta) async {
    final w = zb;
    if (w == null) return;
    const uid = '00000000-0000-4000-8000-00000000c0de';
    if (counterPublic.isEmpty) {
      final v = DateTime.now().toUtc().toIso8601String();
      await w.mutate('counter_public', 'INSERT', {'uid': uid},
          {'uid': uid, 'value': delta, 'inserted_at': v, 'updated_at': v});
    } else {
      await w.mutate('counter_public', 'UPDATE', {'uid': uid},
          {'value': (counterPublic['value'] ?? 0) + delta});
    }
    await _refreshAll(changedTables: ['counter_public']);
  }

  Future<void> _bumpTenantCounter(int delta) async {
    final w = zb;
    if (w == null) return;
    final String uid = counterTenant['uid'] ??
        '00000000-0000-4000-8000-8b0b2beac0df'; // hardcoded alice tenant ID for now
    if (counterTenant.isEmpty) {
      final v = DateTime.now().toUtc().toIso8601String();
      await w.mutate('counter_tenant', 'INSERT', {'uid': uid}, {
        'uid': uid,
        'tenant_id': tenant,
        'value': delta,
        'inserted_at': v,
        'updated_at': v
      });
    } else {
      await w.mutate('counter_tenant', 'UPDATE', {'uid': uid},
          {'value': (counterTenant['value'] ?? 0) + delta});
    }
    await _refreshAll(changedTables: ['counter_tenant']);
  }

  Future<void> _createOrder() async {
    final w = zb;
    if (w == null || formUserName.trim().isEmpty || formItem.trim().isEmpty) {
      return;
    }
    String userId = '';
    final existingUser = users.firstWhere(
        (u) => u['name'] == formUserName.trim(),
        orElse: () => null);
    if (existingUser == null) {
      userId = "${DateTime.now().millisecondsSinceEpoch}-user";
      final v = DateTime.now().toUtc().toIso8601String();
      await w.mutate('app_users', 'INSERT', {'uid': userId}, {
        'uid': userId,
        'name': formUserName.trim(),
        'tenant_id': tenant,
        'inserted_at': v,
        'updated_at': v
      });
    } else {
      userId = existingUser['uid'];
    }

    final v = DateTime.now().toUtc().toIso8601String();
    final count = int.tryParse(formCount) ?? 1;
    // the composite key: (user_id, item)
    await w.mutate('app_orders', 'INSERT', {
      'user_id': userId,
      'item': formItem.trim()
    }, {
      'user_id': userId,
      'item': formItem.trim(),
      'count': count,
      'note': formNote.trim().isEmpty ? null : formNote.trim(),
      'tenant_id': tenant,
      'inserted_at': v,
      'updated_at': v
    });

    if (!mounted) return;
    setState(() {
      formUserName = '';
      formItem = '';
      formCount = '1';
      formNote = '';
    });
    await _refreshAll(changedTables: ['app_users', 'app_orders']);
  }

  @override
  void dispose() {
    WidgetsBinding.instance.removeObserver(this);
    isConnected = false;
    reportsSub?.cancel();
    zb?.close();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: const Text('ZeBridge Flutter (C ABI)'),
        centerTitle: true,
      ),
      body: SingleChildScrollView(
        padding: const EdgeInsets.all(16.0),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Text('Principal: alice · Tenant: $tenant',
                style: const TextStyle(fontWeight: FontWeight.bold)),
            if (lastError != null)
              Text(lastError!, style: const TextStyle(color: Colors.red)),
            const SizedBox(height: 24),

            // Counters
            const Text('Two Counters',
                style: TextStyle(fontSize: 20, fontWeight: FontWeight.bold)),
            Row(
              children: [
                Expanded(
                  child: Card(
                    child: Padding(
                      padding: const EdgeInsets.all(16.0),
                      child: Column(
                        children: [
                          const Text('counter_public'),
                          Row(
                            mainAxisAlignment: MainAxisAlignment.center,
                            children: [
                              IconButton(
                                  onPressed: () => _bumpPublicCounter(-1),
                                  icon: const Icon(Icons.remove)),
                              Text('${counterPublic['value'] ?? 0}',
                                  style: const TextStyle(fontSize: 24)),
                              IconButton(
                                  onPressed: () => _bumpPublicCounter(1),
                                  icon: const Icon(Icons.add)),
                            ],
                          ),
                          Text('v: ${counterPublic['updated_at'] ?? '—'}',
                              style: const TextStyle(
                                  fontSize: 10, color: Colors.grey)),
                        ],
                      ),
                    ),
                  ),
                ),
                Expanded(
                  child: Card(
                    child: Padding(
                      padding: const EdgeInsets.all(16.0),
                      child: Column(
                        children: [
                          const Text('counter_tenant'),
                          Row(
                            mainAxisAlignment: MainAxisAlignment.center,
                            children: [
                              IconButton(
                                  onPressed: () => _bumpTenantCounter(-1),
                                  icon: const Icon(Icons.remove)),
                              Text('${counterTenant['value'] ?? 0}',
                                  style: const TextStyle(fontSize: 24)),
                              IconButton(
                                  onPressed: () => _bumpTenantCounter(1),
                                  icon: const Icon(Icons.add)),
                            ],
                          ),
                          Text('v: ${counterTenant['updated_at'] ?? '—'}',
                              style: const TextStyle(
                                  fontSize: 10, color: Colors.grey)),
                        ],
                      ),
                    ),
                  ),
                ),
              ],
            ),

            const SizedBox(height: 32),
            const Text('Users ⟶ Orders',
                style: TextStyle(fontSize: 20, fontWeight: FontWeight.bold)),
            Card(
              child: Padding(
                padding: const EdgeInsets.all(16.0),
                child: Column(
                  children: [
                    TextField(
                      decoration: const InputDecoration(labelText: 'User'),
                      onChanged: (v) => formUserName = v,
                    ),
                    TextField(
                      decoration: const InputDecoration(labelText: 'Item'),
                      onChanged: (v) => formItem = v,
                    ),
                    Row(
                      children: [
                        Expanded(
                            child: TextField(
                          decoration: const InputDecoration(labelText: 'Count'),
                          keyboardType: TextInputType.number,
                          onChanged: (v) => formCount = v,
                        )),
                        const SizedBox(width: 8),
                        Expanded(
                            child: TextField(
                          decoration: const InputDecoration(labelText: 'Note'),
                          onChanged: (v) => formNote = v,
                        )),
                      ],
                    ),
                    const SizedBox(height: 16),
                    ElevatedButton(
                        onPressed: _createOrder, child: const Text('CREATE')),
                    const Divider(),
                    const Text('Orders',
                        style: TextStyle(fontWeight: FontWeight.bold)),
                    ListView.builder(
                      shrinkWrap: true,
                      physics: const NeverScrollableScrollPhysics(),
                      itemCount: orders.length,
                      itemBuilder: (ctx, i) {
                        final o = orders[i];
                        final u = users.firstWhere(
                            (u) => u['uid'] == o['user_id'],
                            orElse: () => {'name': o['user_id']})['name'];
                        return ListTile(
                          title: Text('${o['item']} x ${o['count']}'),
                          subtitle: Text(
                              '$u ${o['note'] != null ? " · " + o['note'] : ""}'),
                        );
                      },
                    )
                  ],
                ),
              ),
            ),

            const SizedBox(height: 32),
            const Text('SQL Console',
                style: TextStyle(fontSize: 20, fontWeight: FontWeight.bold)),
            TextField(
              controller: TextEditingController(text: sqlText),
              maxLines: 3,
              onChanged: (v) => sqlText = v,
              decoration: const InputDecoration(border: OutlineInputBorder()),
            ),
            Row(
              children: [
                ElevatedButton(onPressed: _runSql, child: const Text('Run')),
                const SizedBox(width: 16),
                Row(
                  children: [
                    Checkbox(
                        value: sqlLive,
                        onChanged: (v) => setState(() => sqlLive = v ?? false)),
                    const Text('Live'),
                  ],
                ),
              ],
            ),
            if (sqlError != null)
              Text(sqlError!, style: const TextStyle(color: Colors.red)),
            if (sqlRows.isNotEmpty) ...[
              const SizedBox(height: 8),
              SingleChildScrollView(
                scrollDirection: Axis.horizontal,
                child: DataTable(
                  columns: sqlRows.first.keys
                      .map<DataColumn>((k) => DataColumn(label: Text(k)))
                      .toList(),
                  rows: sqlRows
                      .map<DataRow>((r) => DataRow(
                          cells: r.values
                              .map<DataCell>(
                                  (v) => DataCell(Text(v.toString())))
                              .toList()))
                      .toList(),
                ),
              )
            ]
          ],
        ),
      ),
    );
  }
}
