# pyDATEV

A python module to load, edit, and save DATEV files and manage the
attached documentation files (Belege).


## Potential alternatives 

* python module [FinTech](https://www.joonis.de/de/fintech/doc/)

## State of implementation


| Datenkategorie                        | Status                   |
|---------------------------------------|--------------------------|
| Buchungsstapel                        | version 9-13 implemented (including Belegarchiv v040-v060) |
| Wiederkehrende Buchungen              | not implemented          |
| Buchungstextkonstanten                | not implemented          |
| Sachkontenbeschriftungen              | not implemented          |
| Konto-Notizen                         | not implemented          |
| Debitoren-/Kreditoren                 | not implemented          |
| Textschlüssel                         | not implemented          |
| Zahlungsbedingungen                   | not implemented          |
| Diverse Adressen                      | not implemented          |
| Buchungssätze der Anlagenbuchführung  | not implemented          |
| Filialen der Anlagenbuchführung       | not implemented          |

## Install

```bash
git clone https://github.com/Fjanks/pydatev
cd pydatev
python setup.py install
```

## Usage examples

### Load, edit and save a DATEV file

Suppose we have a DATEV file of category type Buchungsstapel. For the example, lets say we made some postings on account 6450 and later find out / decide that the postings after the first of April should actually go to account 6335. 
```python
import pydatev as datev
import datetime

# Load data
buchungsstapel = datev.Buchungsstapel(filename = './EXTF_Buchungsstapel-incorrect.csv')

# Correct mistake
d = datetime.date(2021,4,1)
for entry in buchungsstapel.data:
    if entry['Kontonummer'] == 6450 and entry['Belegdatum'] > d:
        entry['Kontonummer'] = 6335

# Save data
buchungsstapel.save('./EXTF_Buchungsstapel-correct.csv')
```

### Create a new DATEV file

```python
import pydatev as datev
import datetime

# Create a buchungsstapel
buchungsstapel = datev.Buchungsstapel(
    berater = 1001,
    mandant = 1,
    wirtschaftsjahr_beginn = datetime.date(2021,1,1),
    sachkontennummernlänge = 4,
    datum_von = datetime.date(2021,1,1),
    datum_bis = datetime.date(2021,12,31))

# Add some nonsense data
buchungsstapel.add_buchung(
    umsatz = 34.56,
    soll_haben = 'S',
    konto = '3333',
    gegenkonto = '1111',
    belegdatum = datetime.date(2021,2,1))
buchungsstapel.add_buchung(
    umsatz = 3.66,
    soll_haben = 'S',
    konto = '4683',
    gegenkonto = '9632',
    belegdatum = datetime.date(2021,2,3))
buchungsstapel.add_buchung(
    umsatz = 3567.66,
    soll_haben = 'H',
    konto = '55555',
    gegenkonto = '66666',
    belegdatum = datetime.date(2021,2,14))

# Save to DATEV file
buchungsstapel.save('EXTF_blablub.csv')
```

### Handling documentation files (Belege)

DATEV's Belegtransfer format pairs a Buchungsstapel CSV (the
booking lines) with a `belege.zip` ("Document Package") that
contains the actual documentation files plus a `document.xml`
manifest. Each booking row references its Beleg via the
`Beleglink` column (a `BEDI "<UUID>"` provider-prefix string),
and the same UUID appears as `<document guid="…">` in the
manifest.

pyDATEV implements both sides through three small components:

| Component | Responsibility |
|---|---|
| `Beleg` | A single documentation file plus the metadata that `document.xml` needs (guid, archive filename, blob, belegtyp). Constructed from a path on disk. |
| `Belegarchiv` | File manager: collects `Beleg` objects (idempotent by GUID), writes a `belege.zip` with a consistent `document.xml`, and reads the same back. Can be used stand-alone. |
| `Buchungsstapel` | Owns a `Belegarchiv` at `self.belege`. Attach a Beleg to a row with `bs.add_beleg(entry, path)`; `bs.save(csv)` auto-writes `belege.zip` next to the CSV when needed. |


#### Example: Create a new Buchungsstapel, add one entry, attach a file (Beleg) and save it:
```python
import pydatev, datetime

bs = pydatev.Buchungsstapel(berater=1001, mandant=1,
    wirtschaftsjahr_beginn=datetime.date(2025,1,1),
    sachkontennummernlänge=4,
    datum_von=datetime.date(2025,1,1),
    datum_bis=datetime.date(2025,12,31),
    waehrungskennzeichen='EUR')
entry = bs.add_buchung(umsatz=34.56, soll_haben='S',
    konto='3333', gegenkonto='1111',
    belegdatum=datetime.date(2025,2,1))

# Attach a file (a path or a ready-made pydatev.Beleg). This adds it to
# bs.belege (dedup by GUID) and sets the row's Beleglink column.
bs.add_beleg(entry, './invoice-001.pdf',
             belegtyp=pydatev.BELEGTYP_RECHNUNGSEINGANG)
bs.save('EXTF_buchungsstapel.csv')  # → CSV + belege.zip alongside
```

#### Example: Load a Buchungsstapel with attached files, modify and save it:

```python
import pydatev 

# Load back. belege.zip next to the CSV is picked up automatically.
bs2 = pydatev.Buchungsstapel(filename='EXTF_buchungsstapel.csv')

# … inspect or modify …
for e in bs2.data:
    link = e['Beleglink']                  # 'BEDI "<UUID>"' or empty
    guid = link.split('"')[-2] if '"' in link else None
    beleg = bs2.belege.get_by_guid(guid) if guid else None
    if beleg:
        print(entry['Beleglink'], '->', beleg.filename, beleg.belegtyp)

# Save modified data
bs.save('./EXTF_buchungsstapel_modified.csv')
```

#### Example: Load a Buchungsstapel and extract the attached files:
```python
import os
import pydatev

archive = pydatev.Belegarchiv(filename='./belege.zip')
os.makedirs('./extracted/', exist_ok=True)
for beleg in archive.data:
    beleg.write_to('./extracted/')
```

#### Example: Deriving your own GUID

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


#### Edge cases

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
- **`Belegarchiv.load()` does not validate**: Existing archives are trusted, so a round-trip
`load(zip) → save(zip)` preserves blobs and filenames bit-identically.
- **Stand-alone Belege without a booking**: just call
  `bs.belege.add(pydatev.Beleg(path))` (or use a top-level
  `pydatev.Belegarchiv` without a Buchungsstapel). Useful when
  staging files before the matching booking exists, or when
  re-saving an archive whose bookings are managed elsewhere.
- **Empty archive**: `Belegarchiv.save()` refuses to write an empty
  archive (DATEV consumers reject such files). `Buchungsstapel.save()`
  simply skips the ZIP step if `bs.belege.data` is empty.
