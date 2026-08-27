import 'package:dart_nats/dart_nats.dart';
import 'dart:convert';

void main() async {
  final client = Client();
  await client.connect(Uri.parse('ws://localhost:8080'), connectOption: ConnectOption(user: 'alice', pass: 's3cret'));
  final js = client.jetStream();
  
  final req = {'tenant': 'acme', 'principal': 'alice'};
  final payload = utf8.encode(jsonEncode(req));
  
  try {
    await js.publish('snapshot.request.test_types', payload);
    print('JSON Publish successful');
  } catch (e) {
  }
  
  await Future.delayed(Duration(seconds: 2));
  
  final kv = await js.keyValue('snapshot_descriptors');
  try {
    final entry = await kv.get('test_types');
    if (entry != null) {
      print('Snapshot descriptor generated via JSON!');
    }
  } catch(e) {
    print('Still no descriptor for JSON');
  }
  
  client.close();
}
