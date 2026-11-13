// The durable ledger is JSON text. Preserve byte columns with the same reserved
// marker as libzb's JSON API; restore them before binding or MessagePack encoding.
export function stringifyBinaryJson(value: unknown): string {
  return JSON.stringify(value, function (key, v) {
    // Buffer.toJSON runs before the replacer in Node; inspect the original too.
    const original = this[key];
    if (!(original instanceof Uint8Array)) return v;
    let binary = '';
    for (let i = 0; i < original.length; i += 0x8000) {
      binary += String.fromCharCode(...original.subarray(i, i + 0x8000));
    }
    return { $bin: btoa(binary) };
  });
}

export function parseBinaryJson(text: string): any {
  return JSON.parse(text, (_key, v) => {
    if (!v || typeof v !== 'object' || Array.isArray(v) ||
        Object.keys(v).length !== 1 || typeof v.$bin !== 'string') return v;
    const binary = atob(v.$bin);
    const bytes = new Uint8Array(binary.length);
    for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
    return bytes;
  });
}
