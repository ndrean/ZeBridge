// pg's default Date parser drops sub-millisecond precision. Preserve Postgres's
// full timestamp value so this importer matches libzb and the Python checksum.
export function utcTimestamp(value) {
  const match = /^(\d{4}-\d\d-\d\d) (\d\d:\d\d:\d\d)(?:\.(\d{1,6}))?\+00(?::00)?$/.exec(value);
  if (!match) throw new Error('Expected a UTC Postgres timestamp');
  return `${match[1]}T${match[2]}.${(match[3] ?? '').padEnd(6, '0')}Z`;
}
