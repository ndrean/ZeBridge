// What the archive says about itself: its metadata's layers — the theme must name them.
import 'dart:convert';
import 'package:pmtiles/pmtiles.dart';

Future<void> main(List<String> args) async {
  final a = await PmTilesArchive.from(args.first);
  final meta = await a.metadata;
  final m = meta is String ? jsonDecode(meta) : meta;
  print('zoom ${a.header.minZoom}-${a.header.maxZoom}, tile type ${a.header.tileType}');
  for (final k in ['name', 'description', 'attribution', 'planetiler:version', 'type']) {
    if (m[k] != null) print('$k: ${m[k]}');
  }
  final layers = (m['vector_layers'] as List?) ?? const [];
  print('vector_layers (${layers.length}): ${layers.map((l) => l['id']).join(', ')}');
}
