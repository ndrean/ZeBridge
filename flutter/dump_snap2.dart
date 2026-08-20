import 'package:dart_nats/dart_nats.dart';
import 'dart:convert';
import 'dart:typed_data';

void main() async {
  final client = Client();
  await client.connect(Uri.parse('ws://localhost:8080'), connectOption: ConnectOption(user: 'alice', pass: 's3cret'));
  final js = client.jetStream();
  
  await js.publish('snapshot.request.test_types', Uint8List(0));
  
  final sub = await js.subscribe('init.snap.test_types.>', stream: 'INIT', deliverPolicy: 'new');
  
  sub.stream.listen((msg) {
    try {
      final str = utf8.decode(msg.data);
      if (!str.contains('"operation":"snapshot"') && !str.contains('"schema":')) {
         final json = jsonDecode(str) as List;
         for (var row in json) {
           print(row);
         }
      }
    } catch (e) {
    }
  });
  
  await Future.delayed(Duration(seconds: 3));
  client.close();
}
