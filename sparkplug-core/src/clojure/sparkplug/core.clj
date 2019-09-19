(ns sparkplug.core
  "This namespace provides the main API for writing Spark tasks.

  Most operations in this namespace place the RDD last in the argument list,
  just like Clojure collection functions. This lets you compose them using the
  thread-last macro (`->>`), making it simple to migrate existing Clojure
  code."
  (:refer-clojure :exclude [count distinct filter first group-by into keys map
                            mapcat max min reduce take vals])
  (:require
    [clojure.core :as c]
    [clojure.tools.logging :as log]
    [sparkplug.function :as f]
    [sparkplug.rdd :as rdd]
    [sparkplug.scala :as scala])
  (:import
    org.apache.spark.Partitioner
    (org.apache.spark.api.java
      JavaPairRDD
      JavaRDD
      JavaSparkContext)))


;; ## RDD Transformations

(defn filter
  "Filter the elements of `rdd` to the ones which satisfy the predicate `f`."
  ^JavaRDD
  [f ^JavaRDD rdd]
  (rdd/set-callsite-name
    (.filter rdd (f/fn1 (comp boolean f)))
    (rdd/fn-name f)))


(defn map
  "Map the function `f` over each element of `rdd`. Returns a new RDD
  representing the transformed elements."
  ^JavaRDD
  [f ^JavaRDD rdd]
  (rdd/set-callsite-name
    (.map rdd (f/fn1 f))
    (rdd/fn-name f)))


(defn mapcat
  "Map the function `f` over each element in `rdd` to produce a sequence of
  results. Returns an RDD representing the concatenation of all element
  results."
  ^JavaRDD
  [f ^JavaRDD rdd]
  (rdd/set-callsite-name
    (.flatMap rdd (f/flat-map-fn f))
    (rdd/fn-name f)))


(defn map-partitions
  "Map the function `f` over each partition in `rdd`, producing a sequence of
  results. Returns an RDD representing the concatenation of all the partition
  results. The function will be called with an iterator of the elements of each
  partition."
  ^JavaRDD
  ([f ^JavaRDD rdd]
   (map-partitions f false rdd))
  ^JavaRDD
  ([f preserve-partitioni ^JavaRDD rdd]
   (rdd/set-callsite-name
     (.mapPartitions rdd (f/flat-map-fn f))
     (rdd/fn-name f))))


(defn map-partitions-indexed
  "Map the function `f` over each partition in `rdd`, producing a sequence of
  results. Returns an RDD representing the concatenation of all the partition
  results. The function will be called with the partition index and an iterator
  of the elements of each partition."
  ^JavaRDD
  [f ^JavaRDD rdd]
  (rdd/set-callsite-name
    (.mapPartitionsWithIndex rdd (f/fn2 f) true)
    (rdd/fn-name f)))


(defn distinct
  "Construct an RDD containing only a single copy of each distinct element in
  `rdd`. Optionally accepts a number of partitions to size the resulting RDD
  with."
  ^JavaRDD
  ([^JavaRDD rdd]
   (rdd/set-callsite-name
     (.distinct rdd)))
  ([num-partitions ^JavaRDD rdd]
   (rdd/set-callsite-name
     (.distinct rdd (int num-partitions))
     (int num-partitions))))


(defn sample
  "Generate a randomly sampled subset of `rdd` with roughly `fraction` of the
  original elements. Callers can optionally select whether the sample happens
  with replacement, and a random seed to control the sample."
  ^JavaRDD
  ([fraction ^JavaRDD rdd]
   (rdd/set-callsite-name
     (.sample rdd true (double fraction))
     (double fraction)))
  ^JavaRDD
  ([fraction replacement? ^JavaRDD rdd]
   (rdd/set-callsite-name
     (.sample rdd (boolean replacement?) (double fraction))
     (double fraction)
     (boolean replacement?)))
  ^JavaRDD
  ([fraction replacement? seed ^JavaRDD rdd]
   (rdd/set-callsite-name
     (.sample rdd (boolean replacement?) (double fraction) (long seed))
     (double fraction)
     (boolean replacement?)
     (long seed))))



;; ## Pair RDD Transformations

(defn keys
  "Transform `rdd` by replacing each pair with its key. Returns a new RDD
  representing the keys."
  ^JavaRDD
  [^JavaPairRDD rdd]
  (rdd/set-callsite-name (.keys rdd)))


(defn vals
  "Transform `rdd` by replacing each pair with its value. Returns a new RDD
  representing the values."
  ^JavaRDD
  [^JavaPairRDD rdd]
  (rdd/set-callsite-name (.values rdd)))


(defn key-by
  "Creates pairs from the elements in `rdd` by using `f` to compute a key for
  each value."
  [f ^JavaRDD rdd]
  (rdd/set-callsite-name
    (.mapToPair rdd (f/pair-fn (juxt f identity)))
    (rdd/fn-name f)))


(defn map->pairs
  "Map the function `f` over each element of `rdd`. Returns a new pair RDD
  representing the transformed elements."
  ^JavaPairRDD
  [f ^JavaRDD rdd]
  (rdd/set-callsite-name
    (.mapToPair rdd (f/pair-fn f))
    (rdd/fn-name f)))


(defn mapcat->pairs
  "Map the function `f` over each element in `rdd` to produce a sequence of
  key-value pairs. Returns a new pair RDD representing the concatenation of all
  result pairs."
  ^JavaPairRDD
  [f ^JavaRDD rdd]
  (rdd/set-callsite-name
    (.flatMapToPair rdd (f/pair-flat-map-fn f))
    (rdd/fn-name f)))


(defn map-partitions->pairs
  "Map the function `f` over each partition in `rdd`, producing a sequence of
  key-value pairs. The function will be called with an iterator of the elements
  of the partition."
  ^JavaPairRDD
  ([f ^JavaRDD rdd]
   (map-partitions->pairs f false rdd))
  ^JavaPairRDD
  ([f preserve-partitioning? ^JavaRDD rdd]
   (rdd/set-callsite-name
     (.mapPartitionsToPair
       rdd
       (f/pair-flat-map-fn f)
       (boolean preserve-partitioning?))
     (rdd/fn-name f)
     (boolean preserve-partitioning?))))


(defn map-vals
  "Map the function `f` over each value of the pairs in `rdd`. Returns a new
  pair RDD representing the transformed pairs."
  ^JavaPairRDD
  [f ^JavaPairRDD rdd]
  (rdd/set-callsite-name
    (.mapValues rdd (f/fn1 f))
    (rdd/fn-name f)))


(defn mapcat-vals
  "Map the function `f` over each value of the pairs in `rdd` to produce a
  collection of values. Returns a new pair RDD representing the concatenated
  keys and values."
  ^JavaPairRDD
  [f ^JavaPairRDD rdd]
  (rdd/set-callsite-name
    (.flatMapValues rdd (f/fn1 f))
    (rdd/fn-name f)))


(defn zip-indexed
  "Zip the elements in `rdd` with their indices. Returns a new pair RDD with
  the element/index tuples.

  The ordering is first based on the partition index and then the ordering of
  items within each partition. So the first item in the first partition gets
  index 0, and the last item in the last partition receives the largest index.

  This method needs to trigger a spark job when `rdd` contains more than one
  partition."
  ^JavaPairRDD
  [^JavaRDD rdd]
  (rdd/set-callsite-name
    (.zipWithIndex rdd)))


(defn zip-unique-ids
  "Zip the elements in `rdd` with unique long identifiers. Returns a new pair
  RDD with the element/id tuples.

  Items in the kth partition will get ids `k`, `n+k`, `2*n+k`, ..., where `n`
  is the number of partitions. So the ids won't be sequential and there may be
  gaps, but this method _won't_ trigger a spark job, unlike `zip-indexed`."
  ^JavaPairRDD
  [^JavaRDD rdd]
  (rdd/set-callsite-name
    (.zipWithUniqueId rdd)))



;; ## Multi-RDD Functions

(defn cartesian
  "Construct an RDD representing the cartesian product of two RDDs. Returns a
  new pair RDD containing all combinations of elements between the datasets."
  ^JavaPairRDD
  [^JavaRDD rdd1 ^JavaRDD rdd2]
  (rdd/set-callsite-name
    (.cartesian rdd1 rdd2)))


(defn union
  "Construct a union of the elements in the provided RDDs. Any identical
  elements will appear multiple times."
  ^JavaRDD
  ([^JavaRDD rdd1 ^JavaRDD rdd2]
   (rdd/set-callsite-name
     (.union rdd1 rdd2)))
  ^JavaRDD
  ([^JavaRDD rdd1 ^JavaRDD rdd2 & rdds]
   (rdd/set-callsite-name
     ;; This method signature is a bit tricky to target since
     ;; `JavaSparkContext` also defines `union` on `JavaPairRDD` and
     ;; `JavaDoubleRDD` which extend the base `JavaRDD` type.
     (.union (JavaSparkContext/fromSparkContext (.context rdd1))
             rdd1
             ^java.util.List (list* rdd2 rdds)))))


(defn intersection
  "Construct an RDD representing the intersection of elements which are in both
  RDDs."
  ^JavaRDD
  [^JavaRDD rdd1 ^JavaRDD rdd2]
  (rdd/set-callsite-name
    (.intersection rdd1 rdd2)))


(defn subtract
  "Remove all elements from `rdd1` that are present in `rdd2`."
  ^JavaRDD
  [^JavaRDD rdd1 ^JavaRDD rdd2]
  (rdd/set-callsite-name
    (.subtract rdd1 rdd2)))


(defn subtract-by-key
  "Construct an RDD representing all pairs in `rdd1` for which there is no pair
  with a matching key in `rdd2`."
  ^JavaPairRDD
  [^JavaPairRDD rdd1 ^JavaPairRDD rdd2]
  (rdd/set-callsite-name
    (.subtractByKey rdd1 rdd2)))


(defn cogroup
  "Produe a new RDD containing an element for each key `k` in the given pair
  RDDs mapped to a tuple of the values from all RDDs as lists.

  If the input RDDs have types `(K, A)`, `(K, B)`, and `(K, C)`, the grouped
  RDD will have type `(K, (list(A), list(B), list(C)))`."
  ^JavaPairRDD
  ([^JavaPairRDD rdd1 ^JavaPairRDD rdd2]
   (rdd/set-callsite-name
     (.cogroup rdd1 rdd2)))
  ^JavaPairRDD
  ([^JavaPairRDD rdd1 ^JavaPairRDD rdd2 ^JavaPairRDD rdd3]
   (rdd/set-callsite-name
     (.cogroup rdd1 rdd2 rdd3)))
  ^JavaPairRDD
  ([^JavaPairRDD rdd1 ^JavaPairRDD rdd2 ^JavaPairRDD rdd3 ^JavaPairRDD rdd4]
   (rdd/set-callsite-name
     (.cogroup rdd1 rdd2 rdd3 rdd4))))


(defn cogroup-partitioned
  "Produe a new RDD containing an element for each key `k` in the given pair
  RDDs mapped to a tuple of the values from all RDDs as lists. The resulting
  RDD partitions may be controlled by setting `partitions` to an integer number
  or a `Partitioner` instance.

  If the input RDDs have types `(K, A)`, `(K, B)`, and `(K, C)`, the grouped
  RDD will have type `(K, (List(A), List(B), List(C)))`."
  ^JavaPairRDD
  ([^JavaPairRDD rdd1 ^JavaPairRDD rdd2 partitions]
   (if (instance? Partitioner partitions)
     (rdd/set-callsite-name
       (.cogroup rdd1 rdd2 ^Partitioner partitions)
       (class partitions))
     (rdd/set-callsite-name
       (.cogroup rdd1 rdd2 (int partitions))
       (int partitions))))
  ^JavaPairRDD
  ([^JavaPairRDD rdd1 ^JavaPairRDD rdd2 ^JavaPairRDD rdd3 partitions]
   (if (instance? Partitioner partitions)
     (rdd/set-callsite-name
       (.cogroup rdd1 rdd2 rdd3 ^Partitioner partitions)
       (class partitions))
     (rdd/set-callsite-name
       (.cogroup rdd1 rdd2 rdd3 (int partitions))
       (int partitions))))
  ^JavaPairRDD
  ([^JavaPairRDD rdd1
    ^JavaPairRDD rdd2
    ^JavaPairRDD rdd3
    ^JavaPairRDD rdd4
    partitions]
   (if (instance? Partitioner partitions)
     (rdd/set-callsite-name
       (.cogroup rdd1 rdd2 rdd3 rdd4 ^Partitioner partitions)
       (class partitions))
     (rdd/set-callsite-name
       (.cogroup rdd1 rdd2 rdd3 rdd4 (int partitions))
       (int partitions)))))


(defn join
  "Construct an RDD containing all pairs of elements with matching keys in
  `rdd1` and `rdd2`. Each pair of elements will be returned as a tuple of
  `(k, (v, w))`, where `(k, v)` is in `rdd1` and `(k, w)` is in `rdd2`.

  Performs a hash join across the cluster. Optionally, `partitions` may be
  provided as an integer number or a partitioner instance to control the
  partitioning of the resulting RDD."
  ^JavaPairRDD
  ([^JavaPairRDD rdd1 ^JavaPairRDD rdd2]
   (rdd/set-callsite-name
     (.join rdd1 rdd2)))
  ([^JavaPairRDD rdd1 ^JavaPairRDD rdd2 partitions]
   (if (instance? Partitioner partitions)
     (rdd/set-callsite-name
       (.join rdd1 rdd2 ^Partitioner partitions)
       (class partitions))
     (rdd/set-callsite-name
       (.join rdd1 rdd2 (int partitions))
       (int partitions)))))


(defn left-outer-join
  "Perform a left outer join of `rdd1` and `rdd2`.

  For each element `(k, v)` in `rdd1`, the resulting RDD will either contain
  all pairs `(k, (v, Some(w)))` for `(k, w)` in `rdd2`, or the pair
  `(k, (v, None))` if no elements in `rdd2` have key `k`.

  Hash-partitions the resulting RDD using the existing partitioner/parallelism
  level unless `partitions` is be provided as an integer number or a
  partitioner instance."
  ^JavaPairRDD
  ([^JavaPairRDD rdd1 ^JavaPairRDD rdd2]
   (rdd/set-callsite-name
     (.leftOuterJoin rdd1 rdd2)))
  ([^JavaPairRDD rdd1 ^JavaPairRDD rdd2 partitions]
   (if (instance? Partitioner partitions)
     (rdd/set-callsite-name
       (.leftOuterJoin rdd1 rdd2 ^Partitioner partitions)
       (class partitions))
     (rdd/set-callsite-name
       (.leftOuterJoin rdd1 rdd2 (int partitions))
       (int partitions)))))


(defn right-outer-join
  "Perform a right outer join of `rdd1` and `rdd2`.

  For each element `(k, w)` in `rdd2`, the resulting RDD will either contain
  all pairs `(k, (Some(v), w))` for `(k, v)` in `rdd1`, or the pair
  `(k, (None, w))` if no elements in `rdd1` have key `k`.

  Hash-partitions the resulting RDD using the existing partitioner/parallelism
  level unless `partitions` is be provided as an integer number or a
  partitioner instance."
  ^JavaPairRDD
  ([^JavaPairRDD rdd1 ^JavaPairRDD rdd2]
   (rdd/set-callsite-name
     (.rightOuterJoin rdd1 rdd2)))
  ([^JavaPairRDD rdd1 ^JavaPairRDD rdd2 partitions]
   (if (instance? Partitioner partitions)
     (rdd/set-callsite-name
       (.rightOuterJoin rdd1 rdd2 ^Partitioner partitions)
       (class partitions))
     (rdd/set-callsite-name
       (.rightOuterJoin rdd1 rdd2 (int partitions))
       (int partitions)))))


(defn full-outer-join
  "Perform a full outer join of `rdd1` and `rdd2`.

  For each element `(k, v)` in `rdd1`, the resulting RDD will either contain all
  pairs `(k, (Some(v), Some(w)))` for `(k, w)` in `rdd2`, or the pair
  `(k, (Some(v), None))` if no elements in other have key `k`. Similarly, for
  each element `(k, w)` in `rdd2`, the resulting RDD will either contain all
  pairs `(k, (Some(v), Some(w)))` for `v` in `rdd1`, or the pair
  `(k, (None, Some(w)))` if no elements in `rdd1` have key `k`.

  Hash-partitions the resulting RDD using the existing partitioner/parallelism
  level unless `partitions` is be provided as an integer number or a
  partitioner instance."
  ^JavaPairRDD
  ([^JavaPairRDD rdd1 ^JavaPairRDD rdd2]
   (rdd/set-callsite-name
     (.rightOuterJoin rdd1 rdd2)))
  ([^JavaPairRDD rdd1 ^JavaPairRDD rdd2 partitions]
   (if (instance? Partitioner partitions)
     (rdd/set-callsite-name
       (.rightOuterJoin rdd1 rdd2 ^Partitioner partitions)
       (class partitions))
     (rdd/set-callsite-name
       (.rightOuterJoin rdd1 rdd2 (int partitions))
       (int partitions)))))



;; ## Pair RDD Aggregation

(defn group-by
  "Group the elements of `rdd` using a key function `f`. Returns a pair RDD
  with each generated key and all matching elements as a value sequence."
  ^JavaPairRDD
  ([f ^JavaRDD rdd]
   (rdd/set-callsite-name
     (.groupBy rdd (f/fn1 f))
     (rdd/fn-name f)))
  ^JavaPairRDD
  ([f num-partitions ^JavaRDD rdd]
   (rdd/set-callsite-name
     (.groupBy rdd (f/fn1 f) (int num-partitions))
     (rdd/fn-name f)
     num-partitions)))


(defn group-by-key
  "Group the entries in the pair `rdd` by key. Returns a new pair RDD with one
  entry per key, containing all of the matching values as a sequence."
  ^JavaPairRDD
  ([^JavaPairRDD rdd]
   (rdd/set-callsite-name
     (.groupByKey rdd)))
  ^JavaPairRDD
  ([num-partitions ^JavaPairRDD rdd]
   (rdd/set-callsite-name
     (.groupByKey rdd (int num-partitions))
     num-partitions)))


(defn reduce-by-key
  "Aggregate the pairs of `rdd` which share a key by combining all of the
  values with the reducing function `f`. Returns a new pair RDD with one entry
  per unique key, holding the aggregated values."
  ^JavaPairRDD
  [f ^JavaPairRDD rdd]
  (rdd/set-callsite-name
    (.reduceByKey rdd (f/fn2 f))
    (rdd/fn-name f)))


(defn combine-by-key
  "Combine the elements for each key using a set of aggregation functions.

  If `rdd` contains pairs of `(K, V)`, the resulting RDD will contain pairs of
  type `(K, C)`. Callers must provide three functions:
  - `seq-fn` which turns a V into a C (for example, `vector`)
  - `conj-fn` to add a V to a C (for example, `conj`)
  - `merge-fn` to combine two C's into a single result"
  ^JavaPairRDD
  ([seq-fn conj-fn merge-fn ^JavaPairRDD rdd]
   (rdd/set-callsite-name
     (.combineByKey rdd
                    (f/fn1 seq-fn)
                    (f/fn2 conj-fn)
                    (f/fn2 merge-fn))
     (rdd/fn-name seq-fn)
     (rdd/fn-name conj-fn)
     (rdd/fn-name merge-fn)))
  ^JavaPairRDD
  ([seq-fn conj-fn merge-fn num-partitions ^JavaPairRDD rdd]
   (rdd/set-callsite-name
     (.combineByKey rdd
                    (f/fn1 seq-fn)
                    (f/fn2 conj-fn)
                    (f/fn2 merge-fn)
                    (int num-partitions))
     (rdd/fn-name seq-fn)
     (rdd/fn-name conj-fn)
     (rdd/fn-name merge-fn)
     num-partitions)))


(defn sort-by-key
  "Reorder the elements of `rdd` so that they are sorted according to their
  natural order or the given comparator `f` if provided. The result may be
  ordered ascending or descending, depending on `ascending?`."
  ^JavaPairRDD
  ([^JavaPairRDD rdd]
   (rdd/set-callsite-name
     (.sortByKey rdd true)))
  ^JavaPairRDD
  ([ascending? ^JavaPairRDD rdd]
   (rdd/set-callsite-name
     (.sortByKey rdd (boolean ascending?))
     (boolean ascending?)))
  ^JavaPairRDD
  ([compare-fn ascending? ^JavaPairRDD rdd]
   (rdd/set-callsite-name
     (.sortByKey rdd
                 (f/comparator-fn compare-fn)
                 (boolean ascending?))
     (rdd/fn-name compare-fn)
     (boolean ascending?)))
  ^JavaPairRDD
  ([compare-fn ascending? num-partitions ^JavaPairRDD rdd]
   (rdd/set-callsite-name
     (.sortByKey rdd
                 (f/comparator-fn compare-fn)
                 (boolean ascending?)
                 (int num-partitions))
     (rdd/fn-name compare-fn)
     (boolean ascending?)
     (int num-partitions))))



;; ## RDD Actions

(defn collect
  "Collect the elements of `rdd` into a vector on the driver. Be careful not to
  realize large datasets with this, as the driver will likely run out of
  memory.

  This is an action that causes computation."
  [^JavaRDD rdd]
  (vec (.collect rdd)))


(defn into
  "Collect the elements of `rdd` into a collection on the driver. Behaves like
  `clojure.core/into`, including accepting an optional transducer.
  Automatically coerces Scala tuples into Clojure vectors.

  Be careful not to realize large datasets with this, as the driver will likely
  run out of memory.

  This is an action that causes computation."
  ([coll ^JavaRDD rdd]
   (into coll identity rdd))
  ([coll xf ^JavaRDD rdd]
   (c/into coll
           (comp (c/map scala/from-tuple) xf)
           (.collect rdd))))


(defn foreach
  "Apply the function `f` to all elements of `rdd`. The function will run on
  the executors where the data resides.

  Consider `foreach-partition` for efficiency if handling an element requires
  costly resource acquisition such as a database connection.

  This is an action that causes computation."
  [f ^JavaRDD rdd]
  (.foreach rdd (f/void-fn f)))


(defn foreach-partition
  "Apply the function `f` to all elements of `rdd` by calling it with a
  sequence of each partition's elements. The function will run on the executors
  where the data resides.

  This is an action that causes computation."
  [f ^JavaRDD rdd]
  (.foreachPartition rdd (f/void-fn (comp f iterator-seq))))


(defn count
  "Count the number of elements in `rdd`.

  This is an action that causes computation."
  [^JavaRDD rdd]
  (.count rdd))


(defn first
  "Find the first element of `rdd`.

  This is an action that causes computation."
  [^JavaRDD rdd]
  (.first rdd))


(defn min
  "Find the minimum element in `rdd` in the ordering defined by `compare-fn`.

  This is an action that causes computation."
  ([^JavaRDD rdd]
   (min compare rdd))
  ([compare-fn ^JavaRDD rdd]
   (.min rdd (f/comparator-fn compare-fn))))


(defn max
  "Find the maximum element in `rdd` in the ordering defined by `compare-fn`.

  This is an action that causes computation."
  ([^JavaRDD rdd]
   (max compare rdd))
  ([compare-fn ^JavaRDD rdd]
   (.max rdd (f/comparator-fn compare-fn))))


(defn take
  "Take the first `n` elements of the RDD.

  This currently scans the partitions _one by one_ on the **driver**, so it
  will be slow if a lot of elements are required. In that case, use `collect`
  to get the whole RDD instead.

  This is an action that causes computation."
  [n ^JavaRDD rdd]
  (.take rdd (int n)))


(defn take-ordered
  "Take the first `n` (smallest) elements from this RDD as defined by the
  elements' natural order or specified comparator.

  This currently scans the partitions _one by one_ on the **driver**, so it
  will be slow if a lot of elements are required. In that case, use `collect`
  to get the whole RDD instead.

  This is an action that causes computation."
  ([n ^JavaRDD rdd]
   (.takeOrdered rdd (int n)))
  ([n compare-fn ^JavaRDD rdd]
   (.takeOrdered rdd (int n) (f/comparator-fn compare-fn))))


(defn reduce
  "Aggregate the elements of `rdd` using the function `f`. The reducing
  function must accept two arguments and should be commutative and associative
  so that it can be computed correctly in parallel.

  This is an action that causes computation."
  [f ^JavaRDD rdd]
  (.reduce rdd (f/fn2 f)))


(defn fold
  "Aggregate the elements of each partition in `rdd`, followed by the results
  for all the partitions, by using the given associative function `f` and a
  neutral `zero` value.

  This is an action that causes computation."
  [f zero ^JavaRDD rdd]
  (.fold rdd zero (f/fn2 f)))


(defn aggregate
  "Aggregate the elements of each partition in `rdd` using `aggregator`, then
  merge the results for all partitions using `combiner`. Both functions will be
  seeded with the neutral `zero` value.

  This is an action that causes computation."
  [aggregator combiner zero ^JavaRDD rdd]
  (.aggregate rdd zero (f/fn2 aggregator) (f/fn2 combiner)))



;; ## Pair RDD Actions

(defn lookup
  "Find all values in the `rdd` pairs whose keys is `k`. The key must be
  serializable with the Java serializer (not Kryo) for this to work.

  This is an action that causes computation."
  [^JavaPairRDD rdd k]
  (vec (.lookup rdd k)))


(defn count-by-key
  "Count the distinct key values in `rdd`. Returns a map of keys to integer
  counts.

  This is an action that causes computation."
  [^JavaPairRDD rdd]
  (into {} (.countByKey rdd)))


(defn count-by-value
  "Count the distinct values in `rdd`. Returns a map of values to integer
  counts.

  This is an action that causes computation."
  [^JavaPairRDD rdd]
  (into {} (.countByValue rdd)))
