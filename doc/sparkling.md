Migrating from Sparkling
========================

Migrating from Sparkling should require very little work - a few functions have
changed names, but the API is extremely similar by design. The major change is
obviously to update the namespaces you're requiring; for example, instead of
requiring `[sparkling.core :as spark]`, require `[sparkplug.core :as spark]`.

Specific changes to be aware of are documented by namespace below.


## `sparkling.conf`

- `get` renamed `get-param`
- `set` renamed `set-param`
- `set-if-missing` renamed `set-param-default`
- `remove` renamed `unset-param`
- `master` no longer sets `"local[*]"` if provided no arguments
- `to-string` renamed `debug-str`


## `sparkling.function`

The names of all of the function interop classes changed and their serialization
is slightly more efficient. Otherwise consumers shouldn't need to change much
here.


## `sparkling.core`

### Spark Contexts
- `spark-context` moved to `sparkplug.context/spark-context`
- `local-spark-context` not implemented
- `default-min-partitions` replaced by `sparkplug.context/info`
- `default-parallelism` replaced by `sparkplug.context/info`
- `stop` moved to `sparkplug.context/stop!`
- `with-context` moved to `sparkplug.context/with-context` and now expects a
  two-element binding vector instead of separate symbol/config args.

### RDD Transformations
- `map-to-pair` renamed `map->pairs`
- `map-values` renamed `map-vals`
- `values` renamed `vals`
- `flat-map` renamed `mapcat`
- `flat-map-to-pair` renamed `mapcat->pairs`
- `flat-map-values` renamed `mapcat-vals`
- `map-partition` renamed `map-partitions`
- `map-partitions-to-pair` renamed `map-partitions->pairs`
- `map-partition-with-index` renamed `map-partitions-indexed`
- `sort-by-key` no longer auto-detects whether the first argument is a
  comparator - explicitly pass the `ascending?` argument to provide a custom
  comparison function
- `sample` has more arities and a different argument signature
- `zip-with-index` renamed `zip-indexed`
- `zip-with-unique-id` renamed `zip-unique-ids`
- `partitionwise-sampled-rdd` not implemented
- `partitioner-aware-union` not implemented
- `intersect-by-key` not implemented

### RDD Actions
- `glom` not implemented
- `collect` returns a vector instead of a mutable Java list
- `collect-map` not implemented, use `(spark/into {} rdd)` instead
- `save-as-text-file` moved to `sparkplug.rdd` namespace
- `histogram` not implemented

### RDD Construction
- `parallelize`/`into-rdd` moved to `sparkplug.rdd/parallelize`
- `parallelize-pairs`/`into-pair-rdd` moved to `sparkplug.rdd/parallelize-pairs`
- `text-file` moved to `sparkplug.rdd/text-file`
- `whole-text-files` moved to `sparkplug.rdd` namespace

### RDD Partitioning
- `hash-partitioner` moved to `sparkplug.rdd` namespace
- `partitions` moved to `sparkplug.rdd` namespace
- `partitioner` moved to `sparkplug.rdd` namespace
- `partition-by` moved to `sparkplug.rdd` namespace
- `repartition` moved to `sparkplug.rdd` namespace
- `repartition` moved to `sparkplug.rdd` namespace
- `coalesce` moved to `sparkplug.rdd` namespace
- `coalesce-max` not implemented
- `rekey` not implemented

### RDD Persistence
- `STORAGE-LEVELS` moved to `sparkplug.rdd/storage-levels`
- `cache`/`storage-level!` replaced by `sparkplug.rdd/cache!`
- `uncache` moved to `sparkplug.rdd/uncache!`
- `checkpoint` moved to `sparkplug.rdd/checkpoint!`

### Misc
- `tuple` moved to `sparkplug.scala` namespace
- `count-partitions` not implemented
- `tuple-by` not implemented
- `key-by-fn` not implemented
- `rdd-name` replaced by `sparkplug.rdd/name` and `sparkplug.rdd/set-name` for
  the read and write operations, respectively.
