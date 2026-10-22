/// ZB_MEASURE=1 (libzb: the same variable, the same lines): one `measure {json}` log line
/// per chain step, per chain applied and per stream reaching its catch-up target —
/// gen_follow.py's seeding report. The targets come from
/// ZB_CATCHUP_TARGETS=CDC_acme=812,CDC_PUBLIC=0: per stream, the seq of the last message
/// this client's filter matched when the connection began, read by the harness before
/// connecting. Without a fixed target, "what CDC replayed afterwards" never ends under
/// live traffic. Node only: a browser has no environment and never measures.

export interface MeasureCounts { messages: number; bytes: number; events: number; applied: number; gated: number; held: number; failed: number }

interface Target { stream: string; target: number; reached: boolean; counts: MeasureCounts }

export const emptyMeasureCounts = (): MeasureCounts => ({ messages: 0, bytes: 0, events: 0, applied: 0, gated: 0, held: 0, failed: 0 });

/// One transaction attempt. Holds and failures replace an event's tentative outcome;
/// callers publish the snapshot only after commit, or discard it on rollback.
export class MeasureAttempt {
  private outcomes = new Map<object, { count: number; status: 'applied' | 'gated' | 'held' | 'failed' }>();

  constructor(events: { ev: object; gated: boolean }[]) {
    for (const { ev, gated } of events) {
      const old = this.outcomes.get(ev);
      this.outcomes.set(ev, { count: (old?.count ?? 0) + 1, status: gated ? 'gated' : 'applied' });
    }
  }

  held(ev: object) { const entry = this.outcomes.get(ev); if (entry) entry.status = 'held'; }
  failed(ev: object) { const entry = this.outcomes.get(ev); if (entry) entry.status = 'failed'; }

  snapshot(): MeasureCounts {
    const counts = emptyMeasureCounts();
    for (const { count, status } of this.outcomes.values()) {
      counts.events += count;
      counts[status] += count;
    }
    return counts;
  }
}

export class Measure {
  readonly enabled: boolean;
  private targets = new Map<string, Target>();
  private log: (line: string) => void;

  constructor(log: (line: string) => void) {
    this.log = log;
    const env = (globalThis as { process?: { env?: Record<string, string | undefined> } }).process?.env;
    this.enabled = !!env?.ZB_MEASURE;
    if (!this.enabled) return;
    for (const pair of (env?.ZB_CATCHUP_TARGETS ?? '').split(',')) {
      const eq = pair.indexOf('=');
      const target = Number(pair.slice(eq + 1));
      if (eq <= 0 || !Number.isInteger(target)) continue;
      const stream = pair.slice(0, eq);
      // Nothing this client follows was on the stream: no catch-up to wait for.
      this.targets.set(stream, { stream, target, reached: target === 0, counts: emptyMeasureCounts() });
    }
  }

  emit(rec: Record<string, unknown>) {
    if (this.enabled) this.log(`measure ${JSON.stringify(rec)}`);
  }

  /// The target still open on `stream`, if any.
  open(stream: string): Target | undefined {
    const t = this.enabled ? this.targets.get(stream) : undefined;
    return t && !t.reached ? t : undefined;
  }

  /// Whether an event at `seq` on `stream` falls inside an open target.
  counts(stream: string | undefined, seq: number | undefined): boolean {
    const t = stream ? this.open(stream) : undefined;
    return !!t && typeof seq === 'number' && seq > 0 && seq <= t.target;
  }

  add(stream: string, c: Partial<MeasureCounts>) {
    const t = this.open(stream);
    if (!t) return;
    for (const k of Object.keys(c) as (keyof MeasureCounts)[]) t.counts[k] += c[k] ?? 0;
  }

  /// `stream` is at or past its target: one line, once. `via` says how it was seen —
  /// `position` (the stored position passed it) or `pending` (the server had nothing left
  /// for this consumer after a message, so nothing at or below the target is still owed).
  reached(stream: string, via: 'position' | 'pending', position: number) {
    const t = this.open(stream);
    if (!t) return;
    t.reached = true;
    const c = t.counts;
    this.emit({ m: 'catchup', stream, target: t.target, position, via, at_ms: Date.now(), ...c });
  }

  /// Every open target the stored positions have passed — a chain landing past it, or a
  /// position moved without a delivery.
  positions(seq: Record<string, number>) {
    for (const t of this.targets.values()) {
      if (!t.reached && (seq[t.stream] ?? 0) >= t.target) this.reached(t.stream, 'position', seq[t.stream]);
    }
  }
}
