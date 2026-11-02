"""The committed figure of a write crowd, from sensor_events' row count sampled every second (lib.sh count_rate):
rows committed, the best rate over any 10 s, and the span from the first committed row to the last.

    count_rate.py LOG      LOG: lines of `<epoch seconds> <row count>`
"""
import sys

pts = [(float(t), int(n)) for t, n in (l.split() for l in open(sys.argv[1]) if len(l.split()) == 2 and l.split()[1].isdigit())]
if len(pts) < 2:
    sys.exit(print("committed: too few samples"))
base, end = pts[0][1], pts[-1][1]
moved = [t for t, n in pts if n > base]
if not moved:
    sys.exit(print("committed: 0 rows"))
first, last = moved[0], next(t for t, n in pts if n == end)
# Samples are a fresh connection each, so their spacing varies: a window of 8 to 12 s.
rates = [(n2 - n1) / (t2 - t1) for k, (t1, n1) in enumerate(pts) for (t2, n2) in pts[k + 1:] if 8 <= t2 - t1 <= 12]
best = f"{max(rates):,.0f} rows/s" if rates else "n/a"
span = max(last - first, 1e-9)
print(f"committed: {end - base:,} rows in {span:.1f} s from the first committed row to the last: {(end - base) / span:,.0f} rows/s; best 10 s {best}")
