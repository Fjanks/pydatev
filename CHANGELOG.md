# Changelog

All notable changes to the project are documented here.

Format based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
versioning per [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.3.0] - 2026-11-22

### Added

- Support decimal.Decimal besides float values for umsatz.
- New integration test
- Note for developers in README.md

### Fixed

- Loading a filename that does not include a preceding path (like 'EXTF_buchungsstapel.csv' without './' at the beginning) works as expected now. 

### Changed

- Code formatting using ruff.

## [0.2.0]

### Added

- Support for reading and writing documents (Belege) that accompany the datev csv file in the form of a zip archive. 

### Fixed 
- Allow quotation marks in text fields. For example, the field `Beleglink` may contain
  `BEDI "<UUID>"`.

### Changed
- Show a more user-friendly error message when the parsing fails. 
