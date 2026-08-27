import { readFileSync } from 'node:fs';
import { ZeBridge } from 'zb-client-ts';
import { nodeStorage, nodeConnect } from 'zb-client-ts/node';
const R = new URL('../../', import.meta.url).pathname;
const zb = new ZeBridge({ natsUrl:'nats://127.0.0.1:4222', principal:'omar',
  creds: readFileSync(`${R}scripts/native/creds/omar.creds`,'utf8'),
  grammar: JSON.parse(readFileSync(`${R}grammar.json`,'utf8')),
  durable:true, storage:nodeStorage, connect:nodeConnect });
zb.onLog((t:string,d:any,l:string)=>{ if (t.startsWith('cdc.')) return;
  console.log(`[${l}] ${(typeof d==='string'?d:JSON.stringify(d)).slice(0,120)}`);});
await zb.connect(); await new Promise(r=>setTimeout(r, Number(process.env.WAIT_MS ?? 15000)));
const n=async(q:string)=>(await zb.query(q))[0].n;
console.log(`=> users=${await n('SELECT COUNT(*) n FROM users')} salaries=${await n('SELECT COUNT(*) n FROM salaries')} fkHeld=${zb.fkHeldCount}`);
await zb.close(); process.exit(0);
