/// <reference types="vite/client" />

/// Typed so `import.meta.env.VITE_PRINCIPAL` is not `any` — the principal has to match a
/// NATS user exactly, and a typo here is refused at authentication rather than downgraded.
interface ImportMetaEnv {
  readonly VITE_PRINCIPAL?: string;
  readonly VITE_PASSWORD?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
