/// Enrollment order is connection order. A caller's explicit override is the whole
/// list; no implicit hub, region or discovered server is appended here.
export function orderedEndpoints(value: string | readonly string[]): string[] {
  const input = typeof value === 'string' ? value.split(',') : value;
  if (!Array.isArray(input) || input.length === 0) throw new Error('NATS endpoints must be a nonempty list');
  const urls: string[] = [];
  let secure: boolean | undefined;
  for (const item of input) {
    if (typeof item !== 'string') throw new Error('NATS endpoint must be a string');
    const url = item.trim();
    if (!url || /[\x00-\x20\x7f,]/.test(url)) throw new Error('Invalid NATS endpoint');
    const scheme = /^([a-z]+):\/\//i.exec(url)?.[1].toLowerCase();
    if (scheme && !['nats', 'tls', 'ws', 'wss'].includes(scheme)) throw new Error('Invalid NATS endpoint scheme');
    const encrypted = scheme === 'tls' || scheme === 'wss';
    if (secure !== undefined && secure !== encrypted) throw new Error('NATS endpoints must not mix secure and plaintext connections');
    secure = encrypted;
    if (!urls.includes(url)) urls.push(url);
  }
  return urls;
}

export interface EndpointIdentity {
  nats_url?: string;
  nats_ws_url?: string;
  nats_urls?: string[];
  nats_ws_urls?: string[];
}

export function identityEndpointUrl(id: EndpointIdentity, websocket: boolean): string | undefined {
  const list = websocket ? id.nats_ws_urls : id.nats_urls;
  const legacy = websocket ? id.nats_ws_url : id.nats_url;
  return list !== undefined ? orderedEndpoints(list).join(',') : legacy ? orderedEndpoints(legacy).join(',') : undefined;
}

/// Keep both transports in the identity, including on a native host. This is the same
/// version-1 identity file libzb reads and writes.
export function endpointIdentity(value: Record<string, unknown>): EndpointIdentity {
  const result: EndpointIdentity = {};
  for (const key of ['nats_url', 'nats_ws_url'] as const) {
    if (value[key] === undefined) continue;
    if (typeof value[key] !== 'string') throw new Error(`${key} must be a string`);
    result[key] = orderedEndpoints(value[key]).join(',');
  }
  for (const key of ['nats_urls', 'nats_ws_urls'] as const) {
    if (value[key] === undefined) continue;
    if (!Array.isArray(value[key])) throw new Error(`${key} must be an endpoint list`);
    result[key] = orderedEndpoints(value[key]);
  }
  return result;
}
