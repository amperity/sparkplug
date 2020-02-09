## Serialization

A major concern of SparkPlug is reliable and efficient serialization for Spark
programs written in Clojure.

Under the umbrella of serialization, there are two separate problems: task functions,
and task results.


### Task functions

These are the functions you pass to RDD transformations, like map and filter.
When you invoke an action on the resulting RDD, the driver will serialize these
functions and broadcast them to executors. Executors must be able to
deserialize the functions and run them across multiple threads.

Due to challenges of serializing functions, Spark uses built-in Java serialization
for task functions. The main difficulty with Clojure functions is that they have
implicit dependencies on namespaces and Vars being available at runtime. If Clojure
functions are not serialized correctly, your application is bound to crash with
confusing errors like "attempting to call unbound fn". To address this,
SparkPlug takes this approach:
* On the driver side: Any function passed to an RDD transformation (map,
  filter, etc.) is serialized along with a list of the namespaces that it
  implicitly depends on. This list is built by reflecting on the _function
  object_ itself, instead of analyzing code.
* On the executor side: When the function is deserialized, first require each
  of those namespaces to ensure they are available before calling the function.
  It's important to synchronize these requires, because `clojure.core/require`
  is not thread-safe! Without synchronization, it's likely to result in
  non-deterministic "unbound fn" and "unbound Var" errors.


### Task results

This refers to the data produced by executing tasks. Executors will either send
results back to the driver (as in a "collect" action), or pass them on to the
next stage for executors to read again.

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
serializers and have them included in the registry. See the
[sparkplug.kryo](https://cljdoc.org/d/amperity/sparkplug-core/CURRENT/api/sparkplug.kryo)
namespace for details.


## Tips

Since task functions are serialized with `java.io.Serializable`, any Clojure
data _closed over by_ a task function is also serialized this way. If you need
to close over a relatively large piece of Clojure data in a task function, such
as a static lookup table, using a broadcast variable will provide much better
performance because it will use the same serialization path as task results.

If you are caching RDDs of Clojure data, consider using a serialized storage
level. This will use Kryo serialization, and will save a lot of memory on executors.
The tradeoff is that this increases CPU time to access the data.
