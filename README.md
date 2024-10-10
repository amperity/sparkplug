Clojure Spark API
=================

[![CircleCI](https://dl.circleci.com/status-badge/img/gh/amperity/sparkplug/tree/main.svg?style=shield)](https://dl.circleci.com/status-badge/redirect/gh/amperity/sparkplug/tree/main)
[![codecov](https://codecov.io/gh/amperity/sparkplug/branch/master/graph/badge.svg)](https://codecov.io/gh/amperity/sparkplug)
[![cljdoc](https://cljdoc.org/badge/amperity/sparkplug)](https://cljdoc.org/d/amperity/sparkplug/CURRENT)

SparkPlug is a Clojure API for [Apache Spark](http://spark.apache.org/).


## Installation

Library releases are published on Clojars. To use the latest version with
Leiningen, add the following dependency to your project:

[![Clojars Project](https://clojars.org/amperity/sparkplug/latest-version.svg)](https://clojars.org/amperity/sparkplug)

This will pull in the omnibus package, which in turn depends on each subproject
of the same version. You may instead depend on the subprojects directly if you
wish to omit some functionality, such as Spark SQL or Machine Learning
dependencies.


## Usage

The sparkplug-core package provides functions for working with RDDs, broadcasts,
and accumulators with the classic Spark context API.
See the [cljdoc](https://cljdoc.org/d/amperity/sparkplug-core/CURRENT) for API docs.


## License

Licensed under the Apache License, Version 2.0. See the [LICENSE](LICENSE) file
for rights and restrictions.
