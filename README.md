# pyDATEV

A python module to load, edit, and save DATEV files and manage the
attached documentation files (Belege).


## Potential alternatives 

* python module [FinTech](https://www.joonis.de/de/fintech/doc/)

## State of implementation


| Datenkategorie                        | Status                   |
|---------------------------------------|--------------------------|
| Buchungsstapel                        | version 9-13 implemented |
| Belegarchiv (Document Package)        | Document_v040 + v060 implemented (import + export) |
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

A booking can carry an attached documentation file (Beleg) — a PDF
invoice, a scanned receipt, etc. Attach one with `bs.add_beleg(...)`;
`bs.save()` then writes a `belege.zip` next to the CSV automatically.

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

# Load back. belege.zip next to the CSV is picked up automatically.
bs2 = pydatev.Buchungsstapel(filename='EXTF_buchungsstapel.csv')
for e in bs2.data:
    link = e['Beleglink']                  # 'BEDI "<UUID>"' or empty
    guid = link.split('"')[-2] if '"' in link else None
    beleg = bs2.belege.get_by_guid(guid) if guid else None
```

The default Beleg GUID is a deterministic UUIDv8 (RFC 9562, SHA-256)
over (archive_name, blob); pass `guid=` to `add_beleg`/`Beleg` to supply
your own stable identity (e.g. a business key that must survive
re-exports even if the bytes change). The derivation primitive is public
as `pydatev.uuid8_from_sha256(namespace, *parts)`. `Belegarchiv.load()`
does **not** validate — existing archives are trusted, so a round-trip
`load(zip) → save(zip)` preserves blobs and filenames bit-identically.
Standalone Belege without a corresponding Buchung are also supported
(`bs.belege.add(beleg)` or `archive.add(...)` on a stand-alone
`Belegarchiv`).

For a complete walkthrough — architecture, API reference, examples,
and edge cases — see [File-handling.md](./File-handling.md).
