#!/bin/sh
# Smoke: the two phone packages nothing here can run, checked for what they carry, as Gradle
# and npm fetch them. Running them is the by-hand check (smoke/README.md).
set -u
V="$ZB_VERSION"
bad=0
fail() { echo "✗ packages: $1"; bad=1; }
cd "$(mktemp -d)"

# elf <file> <expected `file` words>: present, and built for that architecture
elf() { [ -s "$1" ] || { fail "$1 missing"; return; }; file -b "$1" | grep -q "$2" || fail "$1 is not $2: $(file -b "$1")"; }

# Maven Central: eu.zebridge:zebridge-android:<V>, the AAR with libzb for three ABIs.
M=https://repo1.maven.org/maven2/eu/zebridge/zebridge-android/$V
if curl -fsSL -o zb.aar "$M/zebridge-android-$V.aar" && curl -fsSL -o zb.pom "$M/zebridge-android-$V.pom"; then
  mkdir aar && unzip -q zb.aar -d aar
  [ -s aar/classes.jar ] || fail "the AAR has no classes.jar"
  elf aar/jni/arm64-v8a/libzb.so   "ELF 64-bit.*ARM aarch64"
  elf aar/jni/armeabi-v7a/libzb.so "ELF 32-bit.*ARM"
  elf aar/jni/x86_64/libzb.so      "ELF 64-bit.*x86-64"
  [ $bad = 0 ] && echo "✓ maven: eu.zebridge:zebridge-android:$V, libzb for arm64-v8a, armeabi-v7a, x86_64"
else
  fail "eu.zebridge:zebridge-android:$V is not on Maven Central"
fi

# npm: @zebridge/react-native@<V>, libzb for iOS (device, simulator) and the three Android ABIs.
was=$bad
if npm pack --silent "@zebridge/react-native@$V" >/dev/null 2>&1; then
  tar xzf zebridge-react-native-"$V".tgz
  P=package
  for slice in ios-arm64 ios-arm64_x86_64-simulator; do
    a="$P/ios/ZbCore.xcframework/$slice/libzb.a"
    [ -s "$a" ] && file -b "$a" | grep -q "ar archive" || fail "$a missing or not a static library"
  done
  elf "$P/android/zb-android/jniLibs/arm64-v8a/libzb.so"   "ELF 64-bit.*ARM aarch64"
  elf "$P/android/zb-android/jniLibs/armeabi-v7a/libzb.so" "ELF 32-bit.*ARM"
  elf "$P/android/zb-android/jniLibs/x86_64/libzb.so"      "ELF 64-bit.*x86-64"
  [ $bad = "$was" ] && echo "✓ npm: @zebridge/react-native@$V, libzb for iOS (device, simulator) and three Android ABIs"
else
  fail "@zebridge/react-native@$V is not on npm"
fi
exit $bad
