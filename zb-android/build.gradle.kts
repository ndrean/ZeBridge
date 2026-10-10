// The AAR: Kotlin (dev.zebridge.ZeBridge) plus libzb.so per CPU, prebuilt by
// scripts/build.sh into src/main/jniLibs/ — Gradle only packages it, no CMake.
// Published to Maven Central as eu.zebridge:zebridge-android (README, Publish).
import com.vanniktech.maven.publish.AndroidSingleVariantLibrary
import com.vanniktech.maven.publish.JavadocJar
import com.vanniktech.maven.publish.SourcesJar

plugins {
    id("com.android.library") version "9.1.0"
    id("com.vanniktech.maven.publish") version "0.37.0"
    signing
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

mavenPublishing {
    coordinates("eu.zebridge", "zebridge-android", "0.1.0")
    configure(AndroidSingleVariantLibrary(
        javadocJar = JavadocJar.Empty(),
        sourcesJar = SourcesJar.Sources(),
        variant = "release",
    ))
    publishToMavenCentral()
    signAllPublications()
    pom {
        name.set("ZeBridge for Android")
        description.set("The ZeBridge client for Android: an offline-first SQLite replica of PostgreSQL tables, kept in sync over NATS, with libzb built in (arm64-v8a, armeabi-v7a, x86_64).")
        inceptionYear.set("2026")
        url.set("https://github.com/ndrean/zebridge")
        licenses {
            license {
                name.set("The Apache License, Version 2.0")
                url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
                distribution.set("repo")
            }
        }
        developers {
            developer {
                id.set("ndrean")
                name.set("ndrean")
                url.set("https://github.com/ndrean")
            }
        }
        scm {
            url.set("https://github.com/ndrean/zebridge")
            connection.set("scm:git:git://github.com/ndrean/zebridge.git")
            developerConnection.set("scm:git:ssh://git@github.com/ndrean/zebridge.git")
        }
    }
}

// Signed by the gpg command: the key stays in gpg, and pinentry asks for its passphrase.
// The key is named by the signing.gnupg.keyName Gradle property (~/.gradle/gradle.properties).
signing {
    useGpgCmd()
}
