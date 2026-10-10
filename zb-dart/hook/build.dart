// The build hook: hands the Dart or Flutter build the libzb made for its target, from
// prebuilt/<target>/ (scripts/build-prebuilt.sh). Flutter bundles it into the app (an
// Android .so, an iOS or macOS framework, a Linux .so); `dart run` loads it in place.
// lib/src/native.dart binds to it by its asset id, package:zebridge/libzb.
import 'dart:io';

import 'package:code_assets/code_assets.dart';
import 'package:hooks/hooks.dart';

void main(List<String> args) async {
  await build(args, (input, output) async {
    if (!input.config.buildCodeAssets) return;
    final code = input.config.code;
    final os = code.targetOS;
    final arch = switch (code.targetArchitecture) {
      Architecture.arm64 => 'arm64',
      Architecture.x64 => 'x64',
      Architecture.arm => 'arm',
      final other => throw UnsupportedError('zebridge: no libzb for $os/$other'),
    };
    final (target, file) = switch (os) {
      OS.android => ('android_$arch', 'libzbcore.so'),
      OS.iOS when code.iOS.targetSdk == IOSSdk.iPhoneSimulator => ('ios_sim_$arch', 'libzbcore.dylib'),
      OS.iOS => ('ios_$arch', 'libzbcore.dylib'),
      OS.macOS => ('macos_$arch', 'libzbcore.dylib'),
      OS.linux => ('linux_$arch', 'libzbcore.so'),
      _ => throw UnsupportedError('zebridge: no libzb for $os (Android, iOS, macOS and Linux are built)'),
    };
    final lib = input.packageRoot.resolve('prebuilt/$target/$file');
    if (!File.fromUri(lib).existsSync()) {
      throw StateError('zebridge: ${lib.toFilePath()} is missing — in the repository, run zb-dart/scripts/build-prebuilt.sh');
    }
    output.assets.code.add(CodeAsset(
      package: input.packageName,
      name: 'libzb',
      linkMode: DynamicLoadingBundled(),
      file: lib,
    ));
    output.dependencies.add(lib);
  });
}
