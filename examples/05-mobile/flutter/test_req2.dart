import 'package:dart_nats/dart_nats.dart';
import 'dart:convert';
import 'package:messagepack/messagepack.dart';

void main() async {
  final client = Client();
  await client.connect(Uri.parse('ws://localhost:8080'), connectOption: ConnectOption(user: 'alice', pass: 's3cret'));
  final js = client.jetStream();
  
  final p = Packer();
  p.packMapLength(2);
  p.packString('tenant'); p.packString('acme');
  p.packString('principal'); p.packString('alice');
  
  try {
    await js.publish('snapshot.request.test_types', p.takeBytes());
    print('Publish successful');
  } catch (e) {
    print('Publish failed: \$e');
  }
  
  await Future.delayed(Duration(seconds: 2));
  
  final kv = await js.keyValue('snapshot_descriptors');
  try {
    final entry = await kv.get('test_types');
    if (entry != null) {
      print('Snapshot descriptor generated successfully!');
    }
  } catch(e) {
    print('Still no descriptor');
  }
  
  client.close();
}
