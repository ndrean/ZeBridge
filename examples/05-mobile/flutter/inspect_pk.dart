import 'package:dart_nats/dart_nats.dart';
import 'dart:convert';
import 'package:messagepack/messagepack.dart';

void main() async {
  final client = Client();
  await client.connect(Uri.parse('ws://localhost:8080'), connectOption: ConnectOption(user: 'alice', pass: 's3cret'));
  final js = client.jetStream();
  final kv = await js.keyValue('schemas');
  final entry = await kv.get('test_types');
  if (entry != null) {
    dynamic val;
    try {
      val = Unpacker(entry.value).unpackMap();
    } catch (_) {
      val = jsonDecode(utf8.decode(entry.value));
    }
    print(val['sqlite']['pk']);
    print(val['sqlite']['pk_columns']);
  }
  client.close();
}
