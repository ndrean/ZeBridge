import 'package:dart_nats/dart_nats.dart';
import 'package:messagepack/messagepack.dart';

void main() async {
  final client = Client();
  await client.connect(Uri.parse('ws://localhost:8080'), connectOption: ConnectOption(user: 'alice', pass: 's3cret'));
  final js = client.jetStream();
  
  // Try to subscribe to the request stream if possible? No, we publish.
  final p = Packer();
  p.packMapLength(2);
  p.packString('tenant'); p.packString('acme');
  p.packString('principal'); p.packString('alice');
  
  try {
    await js.publish('snapshot.request.test_types', p.takeBytes());
    print('Publish successful');
  } catch (e) {
    print('Publish failed');
  }
  
  final snapKv = await js.keyValue('snapshot_descriptors');
  for(int i=0; i<60; i++) {
    try {
      final entry = await snapKv.get('test_types');
      if (entry != null) {
        print('Got descriptor!');
        break;
      }
    } catch(e) {}
    await Future.delayed(Duration(seconds: 1));
  }
  
  client.close();
}
