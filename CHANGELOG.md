Change Log
==========

All notable changes to this project will be documented in this file, which
follows the conventions of [keepachangelog.com](http://keepachangelog.com/).
This project adheres to [Semantic Versioning](http://semver.org/).

## [Unreleased]
- Sparkplug is now tested with Spark 3.1.3 and Spark 3.2.1.
  Spark 2.4.x and related dependencies were dropped.
- The `sparkplug-ml` sub-project, which has been empty since its creation
  over two years ago, has been removed for now.

## [0.1.8] - 2021-08-06
### Fixed
- `sparkplug.core/union` now works with Spark 3. (#21)
