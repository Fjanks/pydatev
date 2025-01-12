# pyDATEV

A python module to import and export DATEV files.


## Potential alternatives 

* python module [FinTech](https://www.joonis.de/de/fintech/doc/)

## State of implementation


| Datenkategorie                        | Status                   |
|---------------------------------------|--------------------------|
| Buchungsstapel                        | version 9 implemented, except Beleg-import/export  |
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
    belegdatum = datetime.datetime.today().date())
buchungsstapel.add_buchung(
    umsatz = 3.66,
    soll_haben = 'S',
    konto = '4683',
    gegenkonto = '9632',
    belegdatum = datetime.datetime.today().date())
buchungsstapel.add_buchung(
    umsatz = 3567.66,
    soll_haben = 'H',
    konto = '55555',
    gegenkonto = '66666',
    belegdatum = datetime.datetime.today().date())

# Save to DATEV file
buchungsstapel.save('EXTF_blablub.csv')
```
