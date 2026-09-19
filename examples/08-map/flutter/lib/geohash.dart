/// Geohash, the little that the ring needs: a point to its cell, a cell to its box,
/// and the cells around one. Precision 5 is a cell of about 4.9 km by 4.9 km at the
/// equator (narrower with latitude); every cell is a tenant named `c_<geohash>`.
library;

import 'package:latlong2/latlong.dart';

const _b32 = '0123456789bcdefghjkmnpqrstuvwxyz';

class CellBox {
  const CellBox(this.south, this.west, this.north, this.east);
  final double south, west, north, east;
  LatLng get centre => LatLng((south + north) / 2, (west + east) / 2);
  double get latSpan => north - south;
  double get lngSpan => east - west;
}

String geohash(double lat, double lng, {int precision = 5}) {
  var latLo = -90.0, latHi = 90.0, lngLo = -180.0, lngHi = 180.0;
  final out = StringBuffer();
  var bits = 0, ch = 0, even = true;
  while (out.length < precision) {
    if (even) {
      final mid = (lngLo + lngHi) / 2;
      if (lng >= mid) {
        ch = ch * 2 + 1;
        lngLo = mid;
      } else {
        ch = ch * 2;
        lngHi = mid;
      }
    } else {
      final mid = (latLo + latHi) / 2;
      if (lat >= mid) {
        ch = ch * 2 + 1;
        latLo = mid;
      } else {
        ch = ch * 2;
        latHi = mid;
      }
    }
    even = !even;
    if (++bits == 5) {
      out.write(_b32[ch]);
      bits = 0;
      ch = 0;
    }
  }
  return out.toString();
}

CellBox cellBox(String hash) {
  var latLo = -90.0, latHi = 90.0, lngLo = -180.0, lngHi = 180.0;
  var even = true;
  for (final c in hash.split('')) {
    final d = _b32.indexOf(c);
    for (final m in const [16, 8, 4, 2, 1]) {
      if (even) {
        final mid = (lngLo + lngHi) / 2;
        if (d & m != 0) {
          lngLo = mid;
        } else {
          lngHi = mid;
        }
      } else {
        final mid = (latLo + latHi) / 2;
        if (d & m != 0) {
          latLo = mid;
        } else {
          latHi = mid;
        }
      }
      even = !even;
    }
  }
  return CellBox(latLo, lngLo, latHi, lngHi);
}

/// The (2r+1)² cells around the one holding `at`: the ring a phone follows.
Set<String> ring(LatLng at, {int radius = 1, int precision = 5}) {
  final home =
      cellBox(geohash(at.latitude, at.longitude, precision: precision));
  final c = home.centre;
  final out = <String>{};
  for (var i = -radius; i <= radius; i++) {
    for (var j = -radius; j <= radius; j++) {
      out.add(geohash(
          c.latitude + i * home.latSpan, c.longitude + j * home.lngSpan,
          precision: precision));
    }
  }
  return out;
}
