import 'package:dart_nats/dart_nats.dart';
import 'dart:typed_data';

void main() async {
  final client = Client();
  await client.connect(Uri.parse('ws://localhost:8080'), connectOption: ConnectOption(user: 'alice', pass: 's3cret'));
  final js = client.jetStream();
  try {
    await js.publish('snapshot.request.test_types', Uint8List(0));
    print('Publish successful');
  } catch (e) {
    print('Publish failed: \$e');
  }
  client.close();
}
