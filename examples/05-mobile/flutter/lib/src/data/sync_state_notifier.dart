import 'package:flutter/foundation.dart';

class SyncStateNotifier extends ChangeNotifier {
  String status = 'Disconnected';
  bool isConnected = false;
  bool isMigrated = false;
  bool isSnapshot = false;
  bool isCdc = false;

  Map<String, int> tableCounts = {};
  Map<String, String> lastVerbs = {};
  Map<String, int> tableColumns = {};
  Map<String, String> tombstoneColumns = {};
  
  List<String> logs = [];

  void log(String message) {
    final time = DateTime.now().toIso8601String().substring(11, 19);
    logs.insert(0, '[$time] $message');
    if (logs.length > 100) logs.removeLast();
    notifyListeners();
  }

  void updateStatus(String s) {
    status = s;
    log(s);
    if (s == 'Connected & Syncing') isConnected = true;
    notifyListeners();
  }

  void reach(String phase) {
    if (phase == 'migrated') isMigrated = true;
    if (phase == 'snapshot') isSnapshot = true;
    if (phase == 'cdc') isCdc = true;
    notifyListeners();
  }

  void updateTable(String table, {int? count, String? verb, int? colCount}) {
    if (count != null) tableCounts[table] = count;
    if (verb != null) lastVerbs[table] = verb;
    if (colCount != null) tableColumns[table] = colCount;
    notifyListeners();
  }
  
  void removeTable(String table) {
    tableCounts.remove(table);
    lastVerbs.remove(table);
    tableColumns.remove(table);
    notifyListeners();
  }
}
