import { readFileSync } from 'node:fs';
import { ZeBridge } from 'zb-client-ts';
import { nodeStorage, nodeConnect } from 'zb-client-ts/node';
const R = new URL('../../', import.meta.url).pathname;
const zb = new ZeBridge({ natsUrl:'nats://127.0.0.1:4222', principal:'omar',
  creds: readFileSync(`${R}scripts/native/creds/omar.creds`,'utf8'),
  grammar: JSON.parse(readFileSync(`${R}grammar.json`,'utf8')),
  durable:true, storage:nodeStorage, connect:nodeConnect });
let held=0, res=0;
zb.onLog((t:string,d:any)=>{const s=typeof d==='string'?d:'';
  let m=s.match(/(\d+) held for a missing parent/); if(m) held+=+m[1];
  m=s.match(/(\d+) held event\(s\) applied once their parent/); if(m) res+=+m[1];
  if (/Seeded (salaries|users)/.test(s)) console.log(' ', s.slice(0,105));});
await zb.connect(); await new Promise(r=>setTimeout(r, Number(process.env.WAIT_MS ?? 20000)));
const n=async(q:string)=>(await zb.query(q))[0].n;
console.log(`  replica: users(cx)=${await n("SELECT COUNT(*) n FROM users WHERE name LIKE 'cx%'")} salaries=${await n('SELECT COUNT(*) n FROM salaries')}`);
console.log(`  orphaned salaries=${await n('SELECT COUNT(*) n FROM salaries s LEFT JOIN users u ON u.id=s.user_id WHERE u.id IS NULL')} | crossStreamHeld=${held} resolved=${res} stillHeld=${zb.fkHeldCount}`);
const ddl = (await zb.query(`SELECT sql AS n FROM sqlite_master WHERE name='salaries'`))[0]?.n;
console.log('  local DDL has FK:', /FOREIGN KEY/.test(String(ddl)) ? 'yes' : 'NO');
await zb.close(); process.exit(0);
