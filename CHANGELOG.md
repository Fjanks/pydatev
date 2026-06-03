# Changelog

All notable changes to this fork of pyDATEV are documented here.
Format based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
versioning per [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.2.0] — 2026-06-03

### Added

- **Belege support for DATEV Document Packages.** A `Belegarchiv`
  (the `belege.zip` + `document.xml` manifest, schemas v04.0 and
  v06.0) and a file-backed `Beleg` class holding the metadata
  `document.xml` needs (`guid`, `filename`, `blob`, `belegtyp`).
  `Buchungsstapel` gains `bs.add_beleg(entry, filepath, belegtyp=None,
  *, guid=None, archive_name=None)`, which attaches a file, sets the
  row's `Beleglink` column to `BEDI "<UUID>"`, dedups by GUID, and
  auto-writes `belege.zip` next to the CSV on `save()` (reading it
  back on `load()`). A stapel that never attaches a Beleg produces
  byte-identical CSV output and no zip, exactly as the 0.1.0 baseline.
- **Top-level names** `pydatev.Beleg`, `pydatev.Belegarchiv`,
  `pydatev.BELEGTYP_RECHNUNGSEINGANG`,
  `pydatev.BELEGTYP_RECHNUNGSAUSGANG`. The default Beleg GUID is a
  deterministic UUIDv8 (RFC 9562, SHA-256) over
  `(namespace, archive_name, blob)`; pass `guid=` to supply a stable
  identity that survives re-exports even when the bytes change. The
  derivation primitive is public as
  `pydatev.uuid8_from_sha256(namespace, *parts)`.
- **CSV `Text` round-trip with embedded quotes.** `datev2python`
  now un-doubles internal `"` and strips the outer wrapper (inverse
  of `python2datev`'s escape), so a `Beleglink` written as
  `BEDI "<UUID>"` round-trips intact. Tests in
  `tests/test_text_roundtrip.py`.
- User documentation in [File-handling.md](./File-handling.md) and
  tests in `tests/test_belegarchiv.py`.

### Backward compatibility

Additive over the 0.1.0 baseline. Plain `Buchungsstapel` CSV use is
unaffected — same bytes, and no `belege.zip` unless a Beleg is
attached.

## [0.1.0] — historical baseline

Initial published state of the fork (matches upstream
[Fjanks/pydatev](https://github.com/Fjanks/pydatev) at commit
`8ca2369`, "Allow quotation marks in Text values"). Includes
`Buchungsstapel` for DATEV format versions 9–13.
