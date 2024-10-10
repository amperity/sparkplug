Change Log
==========

All notable changes to this project will be documented in this file, which
follows the conventions of [keepachangelog.com](http://keepachangelog.com/).
This project adheres to [Semantic Versioning](http://semver.org/).

## [Unreleased]

...


## [1.1.0] - 2024-10-10

### Changed
- Sparkplug is now tested with Java 11 + Spark 3.1.3, and Java 11 + Spark 3.5.1.
  Java 8 test coverage was dropped.
- Bump Clojure to 1.12.0.
- Update various dependency versions.
- Add clj-kondo linting to test suite.
- Fix bug when serializing functions which close over a boolean value.
  [#27](https://github.com/amperity/sparkplug/issues/27)
  [#28](https://github.com/amperity/sparkplug/pull/28)


## [1.0.0] - 2022-05-31

### Changed
- Update some project dependencies to the latest versions.
- The `sparkplug-sql` sub-project, which has been empty since its creation
  over two years ago, has been removed for now.


## [0.1.9] - 2022-04-25

### Changed
- Sparkplug is now tested with Spark 3.1.3 and Spark 3.2.1.
  Spark 2.4.x and related dependencies were dropped.
- The `sparkplug-ml` sub-project, which has been empty since its creation
  over two years ago, has been removed for now.

### Fixed
- Correctly detect namespace to require when serializing a closure defined
  inside a record type.
  [#23](https://github.com/amperity/sparkplug/pull/23)


## [0.1.8] - 2021-08-06

### Fixed
- `sparkplug.core/union` now works with Spark 3.
  [#21](https://github.com/amperity/sparkplug/pull/21)


[Unreleased]: https://github.com/amperity/sparkplug/compare/1.1.0...HEAD
[1.1.0]: https://github.com/amperity/sparkplug/compare/1.0.0...1.1.0
[1.0.0]: https://github.com/amperity/sparkplug/compare/0.1.9...1.0.0
[0.1.9]: https://github.com/amperity/sparkplug/compare/0.1.8...0.1.9
[0.1.8]: https://github.com/amperity/sparkplug/compare/0.1.7...0.1.8
