# Changelog
All notable changes to this project will be documented in this file.

## v0.3.0+mintel.0.2.0 (2020-07-24)

### Added

* Support for Docker Hub auto-builds
* Support for PUTs, POSTs and DELETEs

### Changed

* **BREAKING**: A call to an object now returns the JSON metadata by default instead of the object's contents. Contents can be requested by setting the `alt` query key to `media` (e.g. `http://my_gcsproxy/my_bucket/my_image.jpg?alt=media`). This brings it inline with the Google API (see official docs [here](https://cloud.google.com/storage/docs/json_api/v1/objects/get)).
* Docker image changes:
  * Now builds in-repo code
  * `distroless-debian10` used as image base for release target
  * Sensible `ENTRYPOINT` and `CMD` values for containers

### Fixed

* CA certificates not working in Docker image

## v0.3.0+mintel.0.1.0 (2020-07-08)

- Initial release
