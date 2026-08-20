import 'package:dart_nats/dart_nats.dart';
import 'dart:convert';
import 'package:messagepack/messagepack.dart';

void main() async {
  final client = Client();
  await client.connect(Uri.parse('ws://localhost:8080'), connectOption: ConnectOption(user: 'alice', pass: 's3cret'));
  final js = client.jetStream();
  final kv = await js.keyValue('snapshot_descriptors');
  final entry = await kv.get('test_types');
  if (entry != null) {
    print('Found descriptor for test_types');
  } else {
    print('No descriptor found');
  }
  client.close();
}
