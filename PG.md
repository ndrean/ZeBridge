# Notes on PostgreSQL Binary Formats

The binary layouts to decode when PostgreSQL sends logical replication through `pgoutput` with `binary 'true'`. PostgreSQL still sends some columns as text. The bridge decodes the types in `pgoutput.zig`; a column of another type (hstore, inet, a range…) suspends its table. The layouts below the JSON section are kept for reference.

> [!NOTE] Postgres binary protocol uses **big‑endian** for integer and float wire formats.

For variable-length types, most values are encoded as `int32 length` followed by `length` bytes (length = -1 means SQL NULL in many array/composite element encodings).

Many composite formats start with small headers (ndigits, weight for NUMERIC; ndim/flags for ARRAY; version for JSONB).

---

## Common simple types (binary payload)

* `bool` — 1 byte: `0x00` = false, else true.
* `int2` — 2 bytes signed big-endian.
* `int4` — 4 bytes signed big-endian.
* `int8` — 8 bytes signed big-endian.
* `float4` — 4 bytes IEEE‑754 big-endian.
* `float8` — 8 bytes IEEE‑754 big-endian.
* `text`, `varchar`, `bpchar` — UTF‑8 bytes (length-prefixed by transport layer when necessary).
* `bytea` — raw bytes.
* `uuid` — 16 bytes (canonical binary UUID layout).

---

## Timestamp & date

* `timestamp` (without tz): 8‑byte signed big-endian integer = microseconds since 2000‑01‑01 00:00:00 (Postgres epoch).
* `timestamptz`: same as timestamp but represents UTC instant.
* `date`: 4‑byte signed int = days since 2000‑01‑01.

---

## NUMERIC (Postgres internal / binary form)

Binary payload layout (int16 = 2 bytes, big‑endian):

```txt
int16 ndigits
int16 weight
int16 sign       -- 0x0000 pos, 0x4000 neg, 0xC000 NaN
int16 dscale
int16 digits[ndigits]  -- each 0..9999 (base-10000), big-endian
```

Interpretation: `digits[0]` is multiplier of `10^(weight*4)`. Each digit is 4 decimal places. Example `-12.30409` -> `ndigits=3, weight=0, sign=0x4000, dscale=5, digits=[12,3040,9000]`.

---

## ARRAY (binary)

```txt
int32 ndim
int32 has_null (0/1)
int32 element_type_oid
for i in 0..ndim-1:
    int32 dim_length
    int32 lower_bound
then elements in row-major order:
    int32 element_length (-1 for NULL)
    <element bytes> (element_length bytes)
```

Note: element bytes are themselves *binary representations* of the element type (not text), so decode those recursively using element_type_oid.

---

## JSON / JSONB

* `json` in binary mode: plain UTF‑8 bytes of JSON text.
* `jsonb` in binary mode: `uint8 version` (1), followed by the JSON text.

---

## HSTORE

* `int32 pairs` then for each pair: `int32 keylen, keybytes, int32 vallen (or -1), valbytes`

---

## INET / CIDR

Binary representation:

```txt
uint8 family (PGSQL_AF_INET=2 / PGSQL_AF_INET6=3)
uint8 masklen (prefix length)
uint8 is_cidr (0/1)
uint8 addrlen (4 or 16)
byte addr[addrlen]
```

---

## RANGE types (general)

Binary form has flags indicating emptiness and whether lower/upper bounds present. If present, each bound appears as a length-prefixed element using the subtype's binary encoding.

---
