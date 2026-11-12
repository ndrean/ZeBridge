// Shared by the TypeScript scenario workers. Production clients never read these variables.
import { existsSync } from 'node:fs';
import { setTimeout as sleep } from 'node:timers/promises';

export function tenantFaultHook() {
  let failed = false;
  return async (point: string) => {
    const hold = process.env.ZB_TEST_TENANT_RENEW_PAUSE_FILE;
    if (point === 'tenant_renew_pause' && hold && existsSync(hold)) {
      console.error('test: tenant renewal held');
      while (existsSync(hold)) await sleep(50);
    }
    if (point === 'tenant_reconnect_fail_once' && process.env.ZB_TEST_TENANT_RECONNECT_FAIL_ONCE && !failed) {
      failed = true;
      throw new Error('test: first tenant reconnect failed');
    }
  };
}
