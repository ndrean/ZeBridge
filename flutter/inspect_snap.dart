import 'package:dart_nats/dart_nats.dart';
import 'dart:convert';
import 'dart:typed_data';

void main() async {
  final client = Client();
  await client.connect(Uri.parse('ws://localhost:8080'), connectOption: ConnectOption(user: 'alice', pass: 's3cret'));
  final js = client.jetStream();
  
  await js.publish('snapshot.request.test_types', Uint8List(0));
  print('Requested snapshot for test_types');
  
  final sub = await js.subscribe('init.snap.test_types.>', stream: 'INIT', deliverPolicy: 'new');
  
  int rowCount = 0;
  sub.stream.listen((msg) {
    try {
      final str = utf8.decode(msg.data);
      if (str.contains('"operation":"snapshot"')) {
         final json = jsonDecode(str);
         final data = json['data'] as List;
         rowCount += data.length;
      } else if (str.contains('"schema":')) {
      } else {
         final json = jsonDecode(str) as List;
         rowCount += json.length;
      }
    } catch (e) {
    }
  });
  
  await Future.delayed(Duration(seconds: 3));
  print('Total rows received: $rowCount');
  client.close();
}
