# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## 1.6.0 - 2022-03-01
### Changed
- default to utf8mb4 charset for connection unless otherwise specified

## 1.5.3 - 2022-02-18
### Changed
- prep() will now escape true and false as "TRUE" and "FALSE"
- fix several Namespace issues