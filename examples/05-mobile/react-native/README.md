# ZeBridge React Native Consumer

This folder contains the React Native implementation of the ZeBridge consumer, mirroring the Web (React/SolidJS) and Flutter UIs.

## Files
- `src/App.tsx`: The complete UI with Counters, Users ⟶ Orders, and the SQL console. It manages local component state triggered by the React Native `ZeBridge` polling.
- `src/ZeBridge.ts`: The JavaScript class that exposes `sync`, `query`, `mutate`, and `poll` to the React components. It interacts with the Native Module.
- `src/ZeBridgeNative.cpp`: An example of how to bind the Zig C ABI (`libzbcore.dylib`/`.so`) to React Native's JSI (JavaScript Interface), providing blazing fast synchronous execution without JSON bridge overhead for simple queries.

## Integration
To fully test this on a device, you need to compile `libzb` (using `zig build`) and link it using a React Native TurboModule or a standard JSI module. 
