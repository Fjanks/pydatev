"""Round-trip tests for Text-format fields.

Regression for the bug where saving and loading a value with embedded
quotes (e.g. `BEDI "<UUID>"`) lost the quotation marks: the serializer
correctly CSV-escapes by doubling internal quotes and wrapping in
outer quotes, but the parser stripped *all* quotes instead of
performing the inverse unescape.
"""

import datetime

import pytest

import pydatev


def _new_stapel():
    return pydatev.Buchungsstapel(
        berater=1001, mandant=1,
        wirtschaftsjahr_beginn=datetime.date(2025, 1, 1),
        sachkontennummernlänge=4,
        datum_von=datetime.date(2025, 1, 1),
        datum_bis=datetime.date(2025, 12, 31),
        waehrungskennzeichen="EUR",
    )


@pytest.mark.parametrize("value", [
    'BEDI "abc-123"',                       # the reported bug shape
    'BEDI "5d8e7f10-3b9a-4d2e-b8a6-9c1f7e0d4321"',
    'plain text',                           # no quotes
    'has "inner" and "more"',               # multiple embedded quotes
    'trailing "',                           # quote at end
    '" leading',                            # quote at start
    'mid"dle',                              # quote in the middle
    '""',                                   # two adjacent quotes as content
])
def test_text_field_roundtrip_preserves_quotes(tmp_path, value):
    bs = _new_stapel()
    e = bs.add_buchung(
        umsatz=1.0, soll_haben="S", konto="3333", gegenkonto="1111",
        belegdatum=datetime.date(2025, 2, 1))
    e["Beleglink"] = value

    csv_path = str(tmp_path / "EXTF_t.csv")
    bs.save(csv_path)

    bs2 = pydatev.Buchungsstapel(filename=csv_path)
    assert bs2.data[0]["Beleglink"] == value
