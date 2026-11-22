"""Integration test

Create a Buchungsstapel with some arbitrary entries, save it to a file, load it and compare if the content is identical.
"""

import datetime
import pytest

import sys

sys.path.append("src")
import pydatev as datev


def test_create_save_and_load_buchungsstapel(tmp_path):

    # Create a buchungsstapel
    buchungsstapel = datev.Buchungsstapel(
        berater=1001,
        mandant=1,
        wirtschaftsjahr_beginn=datetime.date(2021, 1, 1),
        sachkontennummernlänge=4,
        datum_von=datetime.date(2021, 1, 1),
        datum_bis=datetime.date(2021, 12, 31),
    )

    # Add some nonsense data
    buchungsstapel.add_buchung(
        umsatz=34.56, soll_haben="S", konto="3333", gegenkonto="1111", belegdatum=datetime.date(2021, 2, 1)
    )
    buchungsstapel.add_buchung(umsatz=3.66, soll_haben="S", konto="4683", gegenkonto="9632", belegdatum=datetime.date(2021, 2, 3))
    buchungsstapel.add_buchung(
        umsatz=3567.66, soll_haben="H", konto="55555", gegenkonto="66666", belegdatum=datetime.date(2021, 2, 14)
    )

    # Save to DATEV file
    filename = tmp_path / "EXTF_blablub.csv"
    buchungsstapel.save(filename)

    # Load DATEV file
    buchungsstapel2 = datev.Buchungsstapel(filename=filename)

    # Compare values
    assert buchungsstapel._metadata == buchungsstapel2._metadata
    assert buchungsstapel._data == buchungsstapel2._data
