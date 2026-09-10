# ZeBridge iOS Consumer (SwiftUI)

This folder contains the complete native iOS implementation of the ZeBridge consumer using Swift and SwiftUI. It replicates the Web, Flutter, and React Native implementations identically.

## Architecture
- **`ZeBridge-Bridging-Header.h`**: Exposes the `libzb` Zig C ABI to Swift.
- **`ZeBridge.swift`**: The Swift wrapper class mapping the C functions (pointers, C strings) into native Swift types (Dictionaries, JSON decoding) safely.
- **`ContentView.swift`**: A declarative SwiftUI application that provides the visual interface for the Counters, Users ⟶ Orders relation, and the live-updating SQL Console.
- **`ZeBridgeApp.swift`**: App entry point.

## How to Test
1. Compile the Zig C ABI into an iOS static/dynamic library using `zig build -Dtarget=aarch64-ios`.
2. Create a new iOS App project in Xcode.
3. Drop `ZeBridge.swift`, `ContentView.swift`, and `ZeBridgeApp.swift` into the project.
4. Add the bridging header (`ZeBridge-Bridging-Header.h`) and point your Xcode Build Settings (`Objective-C Bridging Header`) to it.
5. Link the generated `libzbcore` binary in the "Frameworks and Libraries" phase.
6. Run the app on a simulator or device to benchmark against the Flutter and React Native variants!
