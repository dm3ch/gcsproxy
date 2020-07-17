# Changelog
All notable changes to this project will be documented in this file.

## Unreleased

### Added

* Support for Docker Hub auto-builds

### Changed

* Docker image changes:
  * Now builds in-repo code
  * `distroless-debian10` used as image base for release target
  * Sensible `ENTRYPOINT` and `CMD` values for containers

### Fixed

* CA certificates not working in Docker image

## v0.3.0+mintel.0.1.0 (2020-07-08)

- Initial release
