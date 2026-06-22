"""Tests for the Beleg/Belegarchiv support (pydatev._belege +
Buchungsstapel.add_beleg / the auto belege.zip on save/load)."""

import datetime
import os
import uuid
import zipfile
import xml.etree.ElementTree as ET

import pytest

import sys
sys.path.append('src')
import pydatev


# ---------- Helpers ----------

def _make_pdf(dirpath, name="test.pdf", content=b"%PDF-1.4 hello"):
    path = os.path.join(dirpath, name)
    with open(path, "wb") as f:
        f.write(content)
    return path


def _new_stapel():
    return pydatev.Buchungsstapel(
        berater=1001, mandant=1,
        wirtschaftsjahr_beginn=datetime.date(2025, 1, 1),
        sachkontennummernlänge=4,
        datum_von=datetime.date(2025, 1, 1),
        datum_bis=datetime.date(2025, 12, 31),
        waehrungskennzeichen="EUR",
    )


def _beleglink_guid(entry):
    """Extract the GUID from a row's `BEDI "<UUID>"` Beleglink."""
    return entry["Beleglink"].split('"')[-2]


# ---------- Tests ----------

def test_attach_read_roundtrip_v040(tmp_path):
    """bs.add_beleg(entry, path, belegtyp) → save → load → the Beleg is
    recoverable via the row's Beleglink (filename, blob, belegtyp
    preserved).
    """
    pdf = _make_pdf(str(tmp_path), name="doc-001.pdf", content=b"PAYLOAD")

    bs = _new_stapel()
    entry = bs.add_buchung(
        umsatz=10.0, soll_haben="S", konto="3333", gegenkonto="1111",
        belegdatum=datetime.date(2025, 2, 1))
    bs.add_beleg(entry, pdf, belegtyp=pydatev.BELEGTYP_RECHNUNGSEINGANG)

    csv_path = str(tmp_path / "EXTF_t.csv")
    bs.save(csv_path)
    assert (tmp_path / "belege.zip").exists()

    bs2 = pydatev.Buchungsstapel(filename=csv_path)
    assert len(bs2.belege.data) == 1
    beleg = bs2.belege.get_by_guid(_beleglink_guid(bs2.data[0]))
    assert beleg is not None
    assert beleg.filename == "doc-001.pdf"
    assert beleg.blob == b"PAYLOAD"
    assert beleg.belegtyp == pydatev.BELEGTYP_RECHNUNGSEINGANG


def test_add_beleg_accepts_ready_made_beleg(tmp_path):
    """add_beleg also accepts a pre-built Beleg (e.g. one carrying an
    explicit, re-export-stable guid) instead of a path."""
    pdf = _make_pdf(str(tmp_path), name="VV-7.pdf", content=b"X")
    explicit = str(uuid.uuid4())
    bs = _new_stapel()
    entry = bs.add_buchung(
        umsatz=1.0, soll_haben="S", konto="3333", gegenkonto="1111",
        belegdatum=datetime.date(2025, 2, 1))
    beleg = bs.add_beleg(
        entry, pydatev.Beleg(pdf, guid=explicit))
    assert beleg.guid == explicit
    assert _beleglink_guid(entry) == explicit


def test_default_guid_is_stable_across_paths(tmp_path):
    """Same archive_name + same content → same GUID, regardless of
    where the file sits on disk. Downstream auto-attach flows (e.g.
    BuchhaltungsButler matching against an already-uploaded
    Beleg-Archiv) depend on this round-trip stability."""
    dir_a = tmp_path / "run_one"
    dir_b = tmp_path / "run_two"
    dir_a.mkdir()
    dir_b.mkdir()
    pdf_a = _make_pdf(str(dir_a), name="VV-23.pdf", content=b"%PDF-A")
    pdf_b = _make_pdf(str(dir_b), name="VV-23.pdf", content=b"%PDF-A")
    b_a = pydatev.Beleg(filepath=pdf_a)
    b_b = pydatev.Beleg(filepath=pdf_b)
    assert b_a.guid == b_b.guid, (
        f"Default GUID must be stable across paths for the same "
        f"(archive_name, content); got {b_a.guid!r} vs {b_b.guid!r}"
    )
    # Explicit archive_name override matches the implicit one when
    # name AND content are equal.
    pdf_c = _make_pdf(str(dir_a), name="other-on-disk.pdf", content=b"%PDF-A")
    b_c = pydatev.Beleg(filepath=pdf_c, archive_name="VV-23.pdf")
    assert b_c.guid == b_a.guid


def test_default_guid_distinguishes_different_content_same_name(tmp_path):
    """Hybrid GUID derivation: two files filed under the same archive
    name but with different content must get DIFFERENT GUIDs. Under
    the old archive-name-only derivation this was a silent collision
    — Belegarchiv.add() would drop the second one as a "duplicate"
    even though the bytes differed."""
    dir_a = tmp_path / "a"
    dir_b = tmp_path / "b"
    dir_a.mkdir()
    dir_b.mkdir()
    pdf_a = _make_pdf(str(dir_a), name="Rechnung.pdf", content=b"%PDF-CONTENT-1")
    pdf_b = _make_pdf(str(dir_b), name="Rechnung.pdf", content=b"%PDF-CONTENT-2")
    b_a = pydatev.Beleg(filepath=pdf_a)
    b_b = pydatev.Beleg(filepath=pdf_b)
    assert b_a.guid != b_b.guid, (
        "Same archive_name but different content must produce "
        "different GUIDs (no silent filename collision)"
    )


def test_default_guid_is_uuidv8(tmp_path):
    """The auto-derived GUID is a UUIDv8 (RFC 9562, custom): version
    nibble = 8, variant = RFC 4122. Confirms we're not silently
    emitting a uuid5/SHA-1 value."""
    pdf = _make_pdf(str(tmp_path), name="x.pdf", content=b"hello")
    b = pydatev.Beleg(filepath=pdf)
    u = uuid.UUID(b.guid)
    assert u.version == 8, f"expected UUIDv8, got version {u.version}"
    assert u.variant == uuid.RFC_4122


def test_default_guid_distinguishes_same_content_different_name(tmp_path):
    """Hybrid GUID derivation: two byte-identical files filed under
    different archive names get DIFFERENT GUIDs. This preserves
    business-identity-via-filename — e.g. two empty placeholder
    Belege filed under distinct names stay as two separate Belege,
    not collapsed into one."""
    pdf_a = _make_pdf(str(tmp_path), name="Beleg-Q1.pdf", content=b"")
    pdf_b = _make_pdf(str(tmp_path), name="Beleg-Q2.pdf", content=b"")
    b_a = pydatev.Beleg(filepath=pdf_a)
    b_b = pydatev.Beleg(filepath=pdf_b)
    assert b_a.guid != b_b.guid, (
        "Same content but different archive_name must produce "
        "different GUIDs (business identity lives in the filename)"
    )


def test_uuid8_from_sha256_is_public_and_deterministic():
    """The UUIDv8/SHA-256 derivation primitive is public and stable;
    Beleg's default GUID is exactly this primitive over (namespace,
    archive_name, 0x00, blob)."""
    ns = uuid.UUID("5d8e7f10-3b9a-4d2e-b8a6-9c1f7e0d4321")
    one = pydatev.uuid8_from_sha256(ns, b"VV-7", b"\0", b"data")
    two = pydatev.uuid8_from_sha256(ns, b"VV-7", b"\0", b"data")
    assert one == two
    assert uuid.UUID(one).version == 8
    assert uuid.UUID(one).variant == uuid.RFC_4122
    # Different parts → different GUID.
    assert one != pydatev.uuid8_from_sha256(ns, b"VV-8", b"\0", b"data")


def test_dedup_same_guid(tmp_path):
    """Two rows attach the same file → archive contains it once;
    both rows' Beleglink GUIDs match the same Beleg."""
    pdf = _make_pdf(str(tmp_path), name="shared.pdf")
    bs = _new_stapel()
    e1 = bs.add_buchung(
        umsatz=1.0, soll_haben="S", konto="3333", gegenkonto="1111",
        belegdatum=datetime.date(2025, 2, 1))
    e2 = bs.add_buchung(
        umsatz=2.0, soll_haben="S", konto="3333", gegenkonto="1111",
        belegdatum=datetime.date(2025, 2, 1))
    bs.add_beleg(e1, pdf)
    bs.add_beleg(e2, pdf)

    assert len(bs.belege.data) == 1
    assert e1["Beleglink"] == e2["Beleglink"]


def test_orphan_beleg_survives_roundtrip(tmp_path):
    """Standalone Belege (no Buchung references them) stay in
    bs.belege.data through save → load."""
    pdf_referenced = _make_pdf(str(tmp_path), name="ref.pdf")
    pdf_orphan = _make_pdf(str(tmp_path), name="orph.pdf", content=b"orphan")

    bs = _new_stapel()
    entry = bs.add_buchung(
        umsatz=1.0, soll_haben="S", konto="3333", gegenkonto="1111",
        belegdatum=datetime.date(2025, 2, 1))
    bs.add_beleg(entry, pdf_referenced)
    # Add an orphan directly through the archive
    bs.belege.add(pydatev.Beleg(pdf_orphan))

    csv = str(tmp_path / "EXTF_t.csv")
    bs.save(csv)
    bs2 = pydatev.Buchungsstapel(filename=csv)
    names = sorted(d.filename for d in bs2.belege.data)
    assert names == ["orph.pdf", "ref.pdf"]
    # The orphan's GUID is not referenced by the entry's Beleglink.
    linked = {_beleglink_guid(bs2.data[0])}
    orphan = [b for b in bs2.belege.data if b.guid not in linked]
    assert len(orphan) == 1
    assert orphan[0].blob == b"orphan"



def test_backward_compat_no_belege_no_zip(tmp_path):
    """A Buchungsstapel without any Beleg attached must produce only the
    CSV (no belege.zip alongside), preserving pre-Beleg behavior.
    Proxy for determinism: saving twice produces the same bytes.
    """
    bs = _new_stapel()
    bs.add_buchung(
        umsatz=34.56, soll_haben="S", konto="3333", gegenkonto="1111",
        belegdatum=datetime.date(2025, 2, 1))
    bs.add_buchung(
        umsatz=3.66, soll_haben="S", konto="4683", gegenkonto="9632",
        belegdatum=datetime.date(2025, 2, 3))

    csv_a = tmp_path / "EXTF_a.csv"
    csv_b = tmp_path / "EXTF_b.csv"
    bs.save(str(csv_a))
    bs.save(str(csv_b))

    assert csv_a.exists() and csv_b.exists()
    assert not (tmp_path / "belege.zip").exists(), \
        "belege.zip must NOT be written when no Belege are attached"
    assert csv_a.read_bytes() == csv_b.read_bytes()


def test_iterate_contains_remove_keeps_xml_and_zip_consistent(tmp_path):
    """Container protocol + remove(). After removing one Beleg,
    save() must produce a ZIP where document.xml and the binary
    payload agree — no dangling <document> entry and no leftover
    file in the ZIP for the removed Beleg.
    """
    pdf_a = _make_pdf(str(tmp_path), name="a.pdf", content=b"AAA")
    pdf_b = _make_pdf(str(tmp_path), name="b.pdf", content=b"BBB")
    arch = pydatev.Belegarchiv(description="t", generating_system="test")
    ba = arch.add(pydatev.Beleg(pdf_a))
    bb = arch.add(pydatev.Beleg(pdf_b))

    # Container protocol
    assert len(arch) == 2
    assert ba.guid in arch
    assert bb in arch
    names = sorted(b.filename for b in arch)
    assert names == ["a.pdf", "b.pdf"]

    # Remove by guid string
    removed = arch.remove(ba.guid)
    assert removed is ba
    assert len(arch) == 1
    assert ba.guid not in arch
    assert arch.get_by_guid(ba.guid) is None

    # Save → load: document.xml and ZIP must be consistent
    zpath = str(tmp_path / "out.zip")
    arch.save(zpath)
    with zipfile.ZipFile(zpath) as zf:
        zip_names = set(zf.namelist())
        xml_bytes = zf.read("document.xml")
    assert zip_names == {"document.xml", "b.pdf"}
    root = ET.fromstring(xml_bytes)
    ns_uri = root.tag.split("}", 1)[0].lstrip("{")
    doc_filenames = {
        d.find("{{{}}}extension".format(ns_uri)).attrib["name"]
        for d in root.findall(
            "{{{0}}}content/{{{0}}}document".format(ns_uri))}
    assert doc_filenames == {"b.pdf"}, (
        "document.xml must list exactly the files present in the ZIP")

    # Remove by Beleg object reference also works
    arch.remove(bb)
    assert len(arch) == 0

    # Removing an unknown guid raises
    with pytest.raises(KeyError):
        arch.remove("00000000-0000-0000-0000-000000000000")


def test_getitem_and_clear(tmp_path):
    """archive[guid] returns the Beleg or raises KeyError; clear()
    empties both the data list and the internal index together."""
    pdf = _make_pdf(str(tmp_path))
    arch = pydatev.Belegarchiv(description="t")
    b = arch.add(pydatev.Beleg(pdf))

    assert arch[b.guid] is b
    with pytest.raises(KeyError):
        arch["00000000-0000-0000-0000-000000000000"]

    arch.clear()
    assert len(arch) == 0
    assert arch.get_by_guid(b.guid) is None


def test_belegarchiv_standalone_v060(tmp_path):
    """v06.0 schema works for standalone Belegarchiv save/load."""
    pdf = _make_pdf(str(tmp_path))
    arch = pydatev.Belegarchiv(
        description="test", generating_system="pytest",
        schema_version="v06.0",
    )
    arch.add(pydatev.Beleg(pdf, belegtyp=pydatev.BELEGTYP_RECHNUNGSAUSGANG))
    zpath = str(tmp_path / "belege.zip")
    arch.save(zpath)

    with zipfile.ZipFile(zpath) as zf:
        root = ET.fromstring(zf.read("document.xml"))
    ns_uri = root.tag.split("}", 1)[0].lstrip("{")
    assert ns_uri == "http://xml.datev.de/bedi/tps/document/v06.0"

    arch2 = pydatev.Belegarchiv(filename=zpath)
    assert arch2.schema_version == "v06.0"
    assert len(arch2.data) == 1
    assert arch2.data[0].belegtyp == pydatev.BELEGTYP_RECHNUNGSAUSGANG
