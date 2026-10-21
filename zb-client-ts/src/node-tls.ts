import { isIP } from 'node:net';
import { checkServerIdentity } from 'node:tls';

/// nats.js leaves an IP endpoint's tlsName empty. Node's TLS upgrade of an existing
/// socket then defaults its verification name to localhost. Bind it to the selected
/// URL on every attempt, including retries, without changing NATS's server selection.
export function bindEndpointTls(options: any): any {
  const servers = typeof options.servers === 'string' ? options.servers.split(',') : options.servers ?? [];
  const secure = servers.some((s: string) => /^tls:\/\//i.test(s.trim()));
  if (options.tls === null) {
    if (secure) throw new Error('TLS endpoints cannot disable TLS');
    return options;
  }
  // A TLS options object makes nats.js require TLS. Preserve plain dev connections.
  if (!secure && options.tls === undefined) return options;
  let hostname: string | undefined;
  let selection = 0;
  let tlsSelection = -1;
  const customCheck = options.tls?.checkServerIdentity;
  return {
    ...options,
    tls: {
      ...options.tls,
      // IP addresses have no SNI name; verification below still uses the exact IP.
      get servername() {
        if (!hostname) throw new Error('TLS options read before endpoint selection; unsupported NATS transport');
        tlsSelection = selection;
        return !isIP(hostname) ? hostname : undefined;
      },
      checkServerIdentity: (_fallback: string, cert: any) => {
        if (!hostname) return new Error('Endpoint TLS name missing: the pinned @nats-io/transport-node 3.4.0 is required');
        if (tlsSelection !== selection) return new Error('TLS options were copied before this endpoint selection; unsupported NATS transport');
        const expected = hostname;
        return checkServerIdentity(expected, cert) ?? customCheck?.(expected, cert);
      },
    },
    reconnectToServer: (pool: any[], info: any) => {
      const choice = options.reconnectToServer?.(pool, info) ?? null;
      const selected = choice && 'server' in choice ? choice.server : choice ?? pool[0];
      hostname = selected?.hostname;
      selection++;
      return choice;
    },
  };
}
