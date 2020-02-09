Clojure Spark API
=================

[![CircleCI](https://circleci.com/gh/amperity/sparkplug.svg?style=shield&circle-token=8222ffae4136dd0fd585c5f2c361ea9426acee8d)](https://circleci.com/gh/amperity/sparkplug)
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


## Serialization

A major concern of SparkPlug is reliable and efficient serialization for Spark
programs written in Clojure.

Under the umbrella of serialization, there are two separate problems:
* Task functions: the functions you pass to RDD transformations, like map and filter.
  When you invoke an action on the resulting RDD, the driver will serialize these functions
  and broadcast them to executors. Executors must be able to
  deserialize the functions and run them across multiple threads.
* Task results: the data produced by executing tasks. Executors will either send results
  back to the driver (as in a "collect" action), or pass them on to the next stage for
  executors to read again.

Due to challenges of serializing functions, Spark uses built-in Java serialization
for task functions. The main difficulty with Clojure functions is that they have
implicit dependencies on namespaces and Vars being available at runtime. If Clojure
functions are not serialized correctly, your application is bound to crash with
confusing errors like "attempting to call unbound fn". To address this,
SparkPlug uses this approach:
* Any function passed to an RDD transformation (map, filter, etc.) is serialized
  along with a list of namespaces that it refers to. This list is built by
  reflecting on the _function object_ itself, instead of analyzing code.
* When the function is deserialized, require each of those namespaces.
  It's important to synchronize these requires, because `clojure.core/require`
  is not thread-safe! Without synchronization, it's likely to result in
  non-deterministic "unbound fn" and "unbound Var" errors.

For task results of Clojure data, such as keywords, maps, and vectors,
Java serialization with `java.io.Serializable` is very suboptimal.
For example, the keyword `:a` gets encoded to a whopping 218 bytes, and
the empty vector `[]` becomes 405 bytes!

SparkPlug solves this using Spark's support for [Kryo serialization](https://github.com/EsotericSoftware/kryo),
by defining custom serializers and a registrator to handle common Clojure data types.
To use SparkPlug's Kryo serialization, set these Spark properties:

| Property                 | Value                                        |
| ------------------------ | -------------------------------------------- |
| `spark.serializer`       | `org.apache.spark.serializer.KryoSerializer` |
| `spark.kryo.registrator` | `sparkplug.kryo.ClassPathRegistrator`        |

For convencience, SparkPlug's configuration builder functions include these
properties by default.

The registrator is also extensible, so that applications can easily add more
serializers and have them included in the registry.
See the [sparkplug.kryo](https://cljdoc.org/d/amperity/sparkplug-core/CURRENT/api/sparkplug.kryo)
namespace for details.

## License

Licensed under the Apache License, Version 2.0. See the [LICENSE](LICENSE) file
for rights and restrictions.
