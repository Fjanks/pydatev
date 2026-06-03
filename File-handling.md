# Handling documentation files (Belege) in pyDATEV

This document is for users who want to manage **Belege** — the
documentation files (invoices, receipts, contracts, …) that DATEV
expects to ship alongside a Buchungsstapel CSV. For plain
Buchungsstapel CSV handling, see [README.md](./README.md); this
document focuses on the Document-Package side.

---

## Overview

DATEV's Belegtransfer format pairs a Buchungsstapel CSV (the
booking lines) with a `belege.zip` ("Document Package") that
contains the actual documentation files plus a `document.xml`
manifest. Each booking row references its Beleg via the
`Beleglink` column (a `BEDI "<UUID>"` provider-prefix string),
and the same UUID appears as `<document guid="…">` in the
manifest.

pyDATEV implements both sides through three small components:

```text
┌──────────────────┐                ┌──────────────────┐
│   Beleg          │  add(beleg)    │   Belegarchiv    │  save() ┐
│ (file + meta)    │ ─────────────► │ (file manager)   │ ────────┼──► belege.zip
│  .guid           │                │  .data: list[…]  │         │   + document.xml
│  .filename       │                │  .add() (dedup)  │ load()  │
│  .blob           │                │  .save() / load()│ ◄───────┘
│  .belegtyp       │                └────────┬─────────┘
└──────▲───────────┘                         │
       │                              owns   │
       │                                     ▼
       │                            ┌──────────────────┐
       │  bs.add_beleg(entry, …):   │  Buchungsstapel  │  save() ─► EXTF_…csv
       └────────────────────────────┤  .belege         │              + belege.zip
            attach + set Beleglink   │  .data: entries  │
                                    └──────────────────┘
```

| Component | Responsibility |
|---|---|
| `Beleg` | A single documentation file plus the metadata that `document.xml` needs (guid, archive filename, blob, belegtyp). Constructed from a path on disk. |
| `Belegarchiv` | File manager: collects `Beleg` objects (idempotent by GUID), writes a `belege.zip` with a consistent `document.xml`, and reads the same back. Can be used stand-alone. |
| `Buchungsstapel` | Owns a `Belegarchiv` at `self.belege`. Attach a Beleg to a row with `bs.add_beleg(entry, path)`; `bs.save(csv)` auto-writes `belege.zip` next to the CSV when needed. |

---

## Import

Everything is reachable from the top-level `pydatev` package:

```python
import pydatev
# pydatev.Buchungsstapel, pydatev.Beleg, pydatev.Belegarchiv,
# pydatev.BELEGTYP_RECHNUNGSEINGANG, pydatev.BELEGTYP_RECHNUNGSAUSGANG,
# pydatev.uuid8_from_sha256
```

or with `from`-imports:

```python
from pydatev import Buchungsstapel, Beleg, Belegarchiv
from pydatev import BELEGTYP_RECHNUNGSEINGANG
```

The Beleg classes are implemented in `pydatev._belege` (an internal
module); import them from the package, not from `_belege` directly.

---

## Requirements

- **Python**: 3.6 or later (matches pyDATEV's base requirement). No
  new third-party dependencies are introduced — Beleg handling uses
  only the standard library (`zipfile`, `xml.etree.ElementTree`,
  `uuid`, `os`, `re`, `hashlib`).
- **Disk-only input**: `Beleg(...)` reads from a file path. If your
  Beleg content lives in memory (e.g. a database blob), materialise
  it to a temporary file first and pass the path.
- **Schema-conformant GUIDs**: every Beleg needs a 36-character UUID
  matching the DATEV Belegtransfer schema constraint. pyDATEV derives
  one automatically (UUIDv8 from SHA-256 of namespace + archive name +
  blob — see `uuid8_from_sha256`) if you don't supply one explicitly.
  The result is stable across re-exports of the same (name, content)
  and distinguishes both filename collisions and content changes.

---

## The `Beleg` class

```python
pydatev.Beleg(
    filepath,                  # str or os.PathLike — file on disk
    belegtyp=None,             # see Belegtyp constants below
    guid=None,                 # auto if omitted (UUIDv8 / SHA-256)
    archive_name=None,         # defaults to os.path.basename(filepath)
)
```

The constructor reads `filepath` into memory and sets:

- `.guid` — 36-char UUIDv8 (RFC 9562), derived from SHA-256 of
  `(namespace || archive_name || 0x00 || blob)` when not supplied.
- `.filename` — the name used inside the ZIP archive.
- `.blob` — the file contents as `bytes`.
- `.belegtyp` — `pydatev.BELEGTYP_RECHNUNGSEINGANG` (`"1"`),
  `pydatev.BELEGTYP_RECHNUNGSAUSGANG` (`"2"`), or `None`.

Convenience method:

```python
beleg.write_to(target_dir)     # writes the blob back to disk; returns path
```

Construction raises `pydatev.DatevFormatError` for a malformed GUID or
an invalid belegtyp. The file extension is **not** restricted — DATEV
rejects unsupported types at import time, and there is no reliable
public allowlist to mirror.

### Deriving your own GUID

The default GUID keys on `(archive_name, blob)`, so it changes if the
file bytes change. When you need an identity that survives re-exports
*even if the bytes change* (e.g. matching against an already-uploaded
Beleg-Archiv), pass an explicit `guid=`. The same UUIDv8/SHA-256
primitive pyDATEV uses internally is public:

```python
import uuid, pydatev
NS = uuid.uuid5(uuid.NAMESPACE_URL, "https://example.org/my-app#beleg")
guid = pydatev.uuid8_from_sha256(NS, b"INV-2025-0007")   # keyed on a business id
beleg = pydatev.Beleg("./invoice.pdf", guid=guid)
```

---

## The `Belegarchiv` class

```python
pydatev.Belegarchiv(
    filename=None,             # if given, load from this ZIP
    description="",
    generating_system="pydatev",
    schema_version="v04.0",    # "v04.0" or "v06.0"
    created=None,              # datetime, defaults to now
)
```

Methods:

```python
archive.add(beleg)             # idempotent by guid; returns the stored Beleg
archive.remove(guid_or_beleg)  # remove; returns the removed Beleg; KeyError if absent
archive.clear()                # empty the archive (data + index)
archive.get_by_guid(guid)      # → Beleg or None
archive.save(zip_path)         # writes ZIP + document.xml manifest
archive.load(zip_path)         # populates archive.data from an existing ZIP
```

Container protocol — natural listing and inspection:

```python
len(archive)                   # number of Belege
for beleg in archive: ...      # iterate (same as `for b in archive.data`)
guid in archive                # membership (also accepts a Beleg instance)
archive[guid]                  # indexed access; raises KeyError if absent
```

`archive.data` is the underlying list of `Beleg` objects.

**Dedup semantics**: `add(b)` looks up `b.guid` in the archive. If a
Beleg with that GUID is already present, the existing entry is
returned untouched and the new one is ignored. This means attaching
the same file to several bookings produces one ZIP entry and one
GUID.

**Removal + consistency**: `remove()` drops the Beleg from both
`data` and the internal index. The next `save()` regenerates
`document.xml` and the ZIP payload from `data`, so the two stay in
sync automatically — no dangling `<document>` entry, no leftover
binary in the ZIP. If a Buchungsstapel entry still references the
removed GUID via `Beleglink`, that reference becomes a dangling
link; clear it with `entry['Beleglink'] = None` on each affected
entry.

**Round-trip guarantee**: `load(zip)` does not validate file
extensions or content — whatever is in the archive is preserved
as-is. A subsequent `save(zip)` produces a bit-equivalent archive
(same blobs, same filenames, same GUIDs).

---

## Buchungsstapel integration

A `pydatev.Buchungsstapel` instance owns one `Belegarchiv`,
accessible at `bs.belege`. Attach a Beleg to a booking row with:

```python
bs.add_beleg(entry, filepath, belegtyp=None, *, guid=None, archive_name=None)
```

Behaviour:

- `filepath` may be a path **or** a ready-made `Beleg`. A `Beleg` is
  constructed (when needed), added to `bs.belege` (dedup by GUID), and
  the row's CSV `Beleglink` column is set to `BEDI "<UUID>"`. The
  attached `Beleg` is returned.
- `belegtyp` sets the Beleg's type at attach time
  (`BELEGTYP_RECHNUNGSEINGANG` / `BELEGTYP_RECHNUNGSAUSGANG` / `None`).
- `guid=` / `archive_name=` are forwarded to the `Beleg` constructor —
  pass `guid=` for a re-export-stable business identity.
- `bs.save(csv_path)` writes the CSV; if `bs.belege.data` is
  non-empty, it also writes `belege.zip` in the same directory.
- `pydatev.Buchungsstapel(filename=csv_path)` loads the CSV; if a
  `belege.zip` lives next to it, the archive is loaded into
  `bs.belege`.

To read back which Beleg a row references, parse the GUID out of its
`Beleglink` and look it up in the archive:

```python
link = entry['Beleglink']                  # 'BEDI "<UUID>"' or empty
guid = link.split('"')[-2] if '"' in link else None
beleg = bs.belege.get_by_guid(guid) if guid else None
```

---

## Examples

### Add a single invoice to a booking

```python
import datetime
import pydatev

bs = pydatev.Buchungsstapel(berater=1001, mandant=1,
    wirtschaftsjahr_beginn=datetime.date(2025,1,1),
    sachkontennummernlänge=4,
    datum_von=datetime.date(2025,1,1),
    datum_bis=datetime.date(2025,12,31),
    waehrungskennzeichen='EUR')

entry = bs.add_buchung(umsatz=34.56, soll_haben='S',
    konto='3333', gegenkonto='1111',
    belegdatum=datetime.date(2025,2,1))
bs.add_beleg(entry, './invoice-001.pdf',
             belegtyp=pydatev.BELEGTYP_RECHNUNGSEINGANG)

bs.save('./out/EXTF_buchungsstapel_2025.csv')
# Produces: ./out/EXTF_buchungsstapel_2025.csv + ./out/belege.zip
```

### Bulk-import a folder of PDFs into a stand-alone Belegarchiv

```python
import os
import pydatev

archive = pydatev.Belegarchiv(description="Belege 2025",
                              generating_system="my-tool")
for name in sorted(os.listdir('./pdfs/')):
    if name.lower().endswith('.pdf'):
        archive.add(pydatev.Beleg(os.path.join('./pdfs/', name)))
archive.save('./belege.zip')
```

### Round-trip a Buchungsstapel through pyDATEV and back

```python
import pydatev

bs = pydatev.Buchungsstapel(filename='./EXTF_buchungsstapel_2025.csv')
# bs.belege.data populated from ./belege.zip if it exists

# … inspect or modify …
for entry in bs.data:
    link = entry['Beleglink']
    guid = link.split('"')[-2] if '"' in link else None
    beleg = bs.belege.get_by_guid(guid) if guid else None
    if beleg:
        print(entry['Beleglink'], '->', beleg.filename, beleg.belegtyp)

bs.save('./EXTF_buchungsstapel_2025.csv')
# CSV and belege.zip rewritten — content bit-identical for unchanged
# parts (Beleg blobs are preserved verbatim from load → save).
```

### Extract every Beleg from a loaded archive back to disk

```python
import os
import pydatev

archive = pydatev.Belegarchiv(filename='./belege.zip')
os.makedirs('./extracted/', exist_ok=True)
for beleg in archive.data:
    beleg.write_to('./extracted/')
```

---

## Schema versions

`document.xml` exists in two DATEV schema variants. pyDATEV defaults
to **v04.0** because it is the variant that DATEV partners
(BuchhaltungsButler, Kontolino, …) consistently produce on
round-trip exports, so it has been validated against real consumers.
**v06.0** is the newer DATEV-Belegtransfer revision and is available
for modern DATEV-Online integrations.

```python
archive = pydatev.Belegarchiv(schema_version="v06.0",
                              description="Belege 2025")
```

On load, pyDATEV detects the namespace and selects the matching
schema automatically.

---

## Backward compatibility

Plain Buchungsstapel use is unaffected: a `Buchungsstapel` that never
calls `add_beleg` produces byte-identical CSV output and no
`belege.zip`. The Beleg names (`Beleg`, `Belegarchiv`, `BELEGTYP_*`,
`uuid8_from_sha256`) are exposed at the top level of `pydatev`.

---

## Edge cases

- **Same file on multiple bookings**: `bs.add_beleg(entry, path)` on a
  second booking with byte-identical content under the same archive
  name reuses the existing Beleg — dedup is by GUID, derived as
  UUIDv8 over `(archive_name, sha256(blob))`. One Beleg in the ZIP;
  both bookings share the same `Beleglink` GUID. Different content
  under the same name, or the same content under different names,
  yield distinct GUIDs (no silent collisions). To deliberately keep
  one logical Beleg per booking even when bookings share a file, pass
  a per-booking `guid=`.
- **Orphan Belege on load**: if a `belege.zip` contains documents
  that no booking row references, they remain in `bs.belege.data`
  after load. Round-trip preserves them on the next save.
- **Stand-alone Belege without a booking**: just call
  `bs.belege.add(pydatev.Beleg(path))` (or use a top-level
  `pydatev.Belegarchiv` without a Buchungsstapel). Useful when
  staging files before the matching booking exists, or when
  re-saving an archive whose bookings are managed elsewhere.
- **Beleglink round-trip with embedded quotes**: pyDATEV's CSV `Text`
  parser correctly un-doubles internal `"` on load, so a `Beleglink`
  written as `BEDI "<UUID>"` round-trips intact.
- **Empty archive**: `Belegarchiv.save()` refuses to write an empty
  archive (DATEV consumers reject such files). `Buchungsstapel.save()`
  simply skips the ZIP step if `bs.belege.data` is empty.
