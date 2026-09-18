// Headless: open the PMTiles archive through a URL with the app's own reader and fetch
// tiles at zoom 14 in and out of the first view — what the map does when you pan.
//   dart run tool/tiles_probe.dart https://ze-map-worker.ze-map.workers.dev/france.pmtiles
import 'dart:math';
import 'package:pmtiles/pmtiles.dart';

(int, int) xy(double lat, double lng, int z) {
  final n = 1 << z;
  final x = ((lng + 180) / 360 * n).floor();
  final y = ((1 - log(tan(lat * pi / 180) + 1 / cos(lat * pi / 180)) / pi) / 2 * n).floor();
  return (x, y);
}

Future<void> main(List<String> args) async {
  final url = args.first;
  final t0 = DateTime.now();
  final a = await PmTilesArchive.from(url);
  print('opened in ${DateTime.now().difference(t0).inMilliseconds} ms: zoom ${a.header.minZoom}-${a.header.maxZoom}');
  final spots = {'Nantes centre': (47.2184, -1.5536), 'Nantes 5 km west': (47.2184, -1.62), 'Rennes': (48.117, -1.678), 'Paris': (48.8566, 2.3522), 'Marseille': (43.2965, 5.3698)};
  for (final z in [12, 14]) {
    for (final e in spots.entries) {
      final (x, y) = xy(e.value.$1, e.value.$2, z);
      final t = DateTime.now();
      try {
        final tile = await a.tile(ZXY(z, x, y).toTileId());
        print('  z$z ${e.key.padRight(18)} ${tile.bytes().length.toString().padLeft(7)} bytes in ${DateTime.now().difference(t).inMilliseconds} ms');
      } catch (err) {
        print('  z$z ${e.key.padRight(18)} FAILED: $err');
      }
    }
  }
}
