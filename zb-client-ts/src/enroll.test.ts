import { test } from 'node:test';
import assert from 'node:assert/strict';
import { enrollAt, selectTenant, ZeBridge } from './libzb.ts';

// A stub transport (only createUser is used) and a fetch that answers from a script.
const transport: any = { createUser: () => ({ publicKey: 'UTESTPUBLICKEY', seed: 'SUTESTSEED' }) };
function scripted(...answers: Array<number | 'down'>) {
  const calls: string[] = [];
  globalThis.fetch = (async (url: string) => {
    calls.push(url);
    const a = answers[Math.min(calls.length - 1, answers.length - 1)];
    if (a === 'down') throw new TypeError('fetch failed');
    if (a === 200) return new Response(JSON.stringify({ jwt: 'eyJ.e30.sig', principal: 'demo-x' }), { status: 200 });
    return new Response('{"error":"busy — retry shortly"}', { status: a });
  }) as any;
  return calls;
}

test('enroll: a busy bridge, a proxy refusal and a failed connection are retried, as libzb does', async () => {
  const calls = scripted(503, 429, 'down', 200);
  const id = await enrollAt('https://bridge.example.com/', 'code-1', transport);
  assert.equal(calls.length, 4, 'three retries, then the answer');
  assert.equal(id.principal, 'demo-x');
});

test('enroll: a refusal is final, the invite is not tried again', async () => {
  const calls = scripted(403, 200);
  await assert.rejects(enrollAt('https://bridge.example.com', 'code-2', transport), /refused \(403\)/);
  assert.equal(calls.length, 1);
});

test('enroll: a bridge that stays busy is given up after five attempts', async () => {
  const calls = scripted(503);
  await assert.rejects(enrollAt('https://bridge.example.com', 'code-3', transport), /answered 503/);
  assert.equal(calls.length, 5);
});

test('a single-tenant replica selects the private assignment beside its open membership', () => {
  assert.equal(selectTenant(['_default', 'private'], '_default'), 'private');
  assert.equal(selectTenant(['private', 'shared'], 'shared'), 'private');
  assert.equal(selectTenant(['_default'], '_default'), '_default');
  assert.equal(selectTenant([], '_default'), '');
});

test('connect resolves the private tenant only after the broker authorizes its route', async () => {
  const client = Object.create(ZeBridge.prototype);
  client.config = { grammar: { open_tenant: '_default' } };
  client.nc = {};
  client.readMembership = async () => ['_default', 'private'];
  client.appendLog = () => {};
  let retries = 0;
  client.scheduleMembership = () => { retries++; };
  client.verifyTenantRoute = async () => { throw new Error('old credentials'); };
  await client.resolveTenant();
  assert.equal(client.tenant, '_default');
  assert.equal(retries, 1);
  client.verifyTenantRoute = async (_nc: unknown, tenant: string) => { assert.equal(tenant, 'private'); };
  await client.resolveTenant();
  assert.equal(client.tenant, 'private');
});

test('first assignment requests credentials or retries a failed renewal on the same connection', async () => {
  const client = Object.create(ZeBridge.prototype);
  const nc = { flush: async () => {} };
  client.nc = nc;
  client.tenantValue = '_default';
  client.config = { grammar: { open_tenant: '_default' } };
  client.membershipDelay = 250;
  client.readMembership = async () => ['_default', 'private'];
  client.appendLog = () => {};
  client.fault = async () => {};
  const retries: number[] = [];
  client.scheduleMembership = (delay: number) => retries.push(delay);
  await client.refreshMembership();
  assert.equal(client.credentialsRequired, true);
  assert.deepEqual(retries, []);
  client.identityNow = {};
  client.renewNow = async () => 'failed';
  await client.refreshMembership();
  await client.refreshMembership();
  assert.deepEqual(retries, [250, 500]);
  assert.equal(client.nc, nc);
  assert.equal(client.tenant, '_default');
});

test('an empty tenant mapping and a failed first read give distinct diagnostics', async () => {
  const client = Object.create(ZeBridge.prototype);
  client.config = { principal: 'alice', grammar: { open_tenant: '_default' } };
  client.nc = {};
  const warnings: string[] = [];
  client.appendLog = (_area: string, message: string, level: string) => {
    assert.equal(level, 'WARN');
    warnings.push(message);
  };
  let retries = 0;
  client.scheduleMembership = () => { retries++; };
  client.readMembership = async () => [];
  await client.resolveTenant();
  assert.match(warnings[0], /No tenant mapping for 'alice'.*public tables/);
  assert.equal(retries, 0);
  client.readMembership = async () => { throw new Error('unavailable'); };
  await client.resolveTenant();
  assert.match(warnings[1], /^Tenant resolution deferred: .*unavailable/);
  assert.equal(retries, 1);
});
