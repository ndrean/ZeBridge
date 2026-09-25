// libzb through the native module (ios/ZbNativeModule.swift). Every call returns the
// C ABI's JSON as text; the handle is a string because a u64 does not fit a JS number.
import { NativeModule, requireNativeModule } from 'expo';

declare class ZbNativeModule extends NativeModule {
  connect(optsJson: string): Promise<string>;
  sync(handle: string): Promise<string>;
  poll(handle: string, waitMs: number): Promise<string>;
  query(handle: string, sql: string, paramsJson: string): Promise<string>;
  close(handle: string): Promise<number>;
}

export default requireNativeModule<ZbNativeModule>('ZbNative');
