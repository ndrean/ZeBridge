import 'package:dart_nats/dart_nats.dart';
import 'dart:convert';
import 'package:messagepack/messagepack.dart';

void main() async {
  final client = Client();
  await client.connect(Uri.parse('ws://localhost:8080'), connectOption: ConnectOption(user: 'alice', pass: 's3cret'));
  final js = client.jetStream();
  final req = {'tenant': 'acme', 'principal': 'alice'};
  
  try {
     await js.publish('snapshot.request.test_types', utf8.encode(jsonEncode(req)));
  } catch(e) {}
  
  final snapKv = await js.keyValue('snapshot_descriptors');
  final entry = await snapKv.get('test_types');
  if (entry != null) {
    dynamic val;
    try {
      val = Unpacker(entry.value).unpackMap();
    } catch (_) {
      val = jsonDecode(utf8.decode(entry.value));
    }
    print('Snap ID: \${val['snapshot_id']}');
    print('LSN: \${val['lsn']}');
  }
  client.close();
}
