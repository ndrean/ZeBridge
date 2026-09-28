// The AAR: Kotlin (dev.zebridge.ZeBridge) plus libzb.so per CPU, prebuilt by
// scripts/build.sh into src/main/jniLibs/ — Gradle only packages it, no CMake.
plugins {
    id("com.android.library") version "9.1.0"
}

android {
    namespace = "dev.zebridge"
    compileSdk = 36
    defaultConfig {
        // The native library links Android 10's libc (-Dandroid-api=29 in scripts/build.sh).
        minSdk = 29
        consumerProguardFiles("consumer-rules.pro")
        testInstrumentationRunner = "androidx.test.runner.AndroidJUnitRunner"
    }
    // The device test's dev creds, copied in by scripts/device-test.sh: test APK only,
    // never the AAR.
    sourceSets["androidTest"].assets.srcDir("build/zbtest-assets")
    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }
}

kotlin {
    compilerOptions {
        jvmTarget.set(org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_17)
    }
}

dependencies {
    androidTestImplementation("androidx.test:runner:1.6.2")
    androidTestImplementation("androidx.test.ext:junit:1.2.1")
}
