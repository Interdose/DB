# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## 1.6.4 - 2024-07-30
### Changed
- don't rely on existing global array for direct() call (by PK)
- add DB::$ENABLE_DANGEROUS_METHODS + Insert::on_duplicate_key_update()

## 1.6.2 - 2023-10-13
### Changed
- fix prep() handling of "0" (string with number 0)

## 1.6.1 - 2022-12-01
### Changed
- add NOW() function

## 1.6.0 - 2022-03-01
### Changed
- default to utf8mb4 charset for connection unless otherwise specified

## 1.5.3 - 2022-02-18
### Changed
- prep() will now escape true and false as "TRUE" and "FALSE"
- fix several Namespace issues