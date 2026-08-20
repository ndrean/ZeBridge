import 'package:dart_nats/dart_nats.dart';
import 'dart:convert';
import 'package:messagepack/messagepack.dart';

void main() async {
  final client = Client();
  await client.connect(Uri.parse('ws://localhost:8080'), connectOption: ConnectOption(user: 'alice', pass: 's3cret'));
  final js = client.jetStream();
  
  final snapKv = await js.keyValue('snapshots');
  try {
    final entry = await snapKv.get('test_types');
    if (entry != null) {
      print('Got descriptor for test_types!');
      dynamic desc;
      try {
        desc = Unpacker(entry.value).unpackMap();
      } catch (_) {
        desc = jsonDecode(utf8.decode(entry.value));
      }
      print('Snapshot ID: \${desc['snapshot_id']}');
    }
  } catch(e) {
    print('Failed to get descriptor: \$e');
  }
  
  client.close();
}
