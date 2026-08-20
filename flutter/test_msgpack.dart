import 'package:messagepack/messagepack.dart';
void main() {
  var p = Packer();
  p.packString('hello');
  var u = Unpacker(p.takeBytes());
  print(u.unpackString());
}
