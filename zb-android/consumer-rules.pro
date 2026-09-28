# JNI resolves Java_dev_zebridge_Native_* by name: keep the class and its natives.
-keep class dev.zebridge.Native { native <methods>; }
