import { test } from 'node:test';
import assert from 'node:assert/strict';
import { enrollAt } from './libzb.ts';

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
