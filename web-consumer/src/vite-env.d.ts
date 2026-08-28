/// <reference types="vite/client" />

/// Typed so `import.meta.env.VITE_PRINCIPAL` is not `any` — the principal has to match a
/// NATS user exactly, and a typo here is refused at authentication rather than downgraded.
interface ImportMetaEnv {
  readonly VITE_PRINCIPAL?: string;
  readonly VITE_PASSWORD?: string;
  /// Where NATS's websocket and the bridge's HTTP actually are. Unset means the
  /// native dev loop's ports (8080 / 9090); the compose stack puts both behind one
  /// nginx origin, so there they are `ws://localhost:8090/nats` and
  /// `http://localhost:8090`.
  /// 'creds' (default, operator/JWT) or 'password' (the pre-operator broker, which
  /// compose runs). The creds file is reachable either way — the broker's acceptance
  /// of it is what differs — so this cannot be detected, only declared.
  readonly VITE_AUTH?: 'creds' | 'password';
  readonly VITE_NATS_URL?: string;
  readonly VITE_BRIDGE_URL?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
