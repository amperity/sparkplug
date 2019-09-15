(ns sparkler.core
  "This namespace provides the main API for writing Spark tasks.

  Most operations in this namespace place the RDD last in the argument list,
  just like Clojure collection functions. This lets you compose them using the
  thread-last macro (`->>`), making it simple to migrate existing Clojure
  code."
  (:refer-clojure :exclude [map mapcat reduce first count take distinct filter
                            group-by values partition-by keys max min])
  (:require
    [clojure.core :as c]
    [clojure.tools.logging :as log]
    [sparkler.function :as f]
    [sparkler.name :as name]
    [sparkler.rdd :as rdd])
  (:import
    (org.apache.spark.api.java
      JavaPairRDD
      JavaRDD
      JavaSparkContext
      StorageLevels)))


;; ## Spark Context

;; TODO: dynamic context var for convenience?

(defn default-min-partitions
  "Default min number of partitions for Hadoop RDDs when not given by user"
  [^JavaSparkContext spark-context]
  (.defaultMinPartitions spark-context))


(defn default-parallelism
  "Default level of parallelism to use when not given by user (e.g. parallelize and makeRDD)."
  [^JavaSparkContext spark-context]
  (.defaultParallelism spark-context))



;; ## RDD Transformations

(defn filter
  "Filter the elements of `rdd` to the ones which satisfy the predicate `f`."
  ^JavaRDD
  [f ^JavaRDD rdd]
  (rdd/set-callsite-name
    (.filter rdd (f/fn1 (comp boolean f)))
    (name/fn-name f)))


(defn map
  "Map the function `f` over each element of `rdd`. Returns a new RDD
  representing the transformed elements."
  ^JavaRDD
  [f ^JavaRDD rdd]
  (rdd/set-callsite-name
    (.map rdd (f/fn1 f))
    (name/fn-name f)))


(defn mapcat
  "Map the function `f` over each element in `rdd` to produce a sequence of
  results. Returns an RDD representing the concatenation of all element
  results."
  ^JavaRDD
  [f ^JavaRDD rdd]
  (rdd/set-callsite-name
    (.flatMap rdd (f/flat-map-fn f))
    (name/fn-name f)))


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
     (name/fn-name f))))


(defn map-partitions-indexed
  "Map the function `f` over each partition in `rdd`, producing a sequence of
  results. Returns an RDD representing the concatenation of all the partition
  results. The function will be called with the partition index and an iterator
  of the elements of each partition."
  ^JavaRDD
  [f ^JavaRDD rdd]
  (rdd/set-callsite-name
    (.mapPartitionsWithIndex rdd (f/fn2 f) true)
    (name/fn-name f)))


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


(defn values
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
    (name/fn-name f)))


(defn map->pairs
  "Map the function `f` over each element of `rdd`. Returns a new pair RDD
  representing the transformed elements."
  ^JavaPairRDD
  [f ^JavaRDD rdd]
  (rdd/set-callsite-name
    (.mapToPair rdd (f/pair-fn f))
    (name/fn-name f)))


(defn mapcat->pairs
  "Map the function `f` over each element in `rdd` to produce a sequence of
  key-value pairs. Returns a new pair RDD representing the concatenation of all
  result pairs."
  ^JavaPairRDD
  [f ^JavaRDD rdd]
  (rdd/set-callsite-name
    (.flatMapToPair rdd (f/pair-flat-map-fn f))
    (name/fn-name f)))


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
     (name/fn-name f)
     (boolean preserve-partitioning?))))


(defn map-values
  "Map the function `f` over each value of the pairs in `rdd`. Returns a new
  pair RDD representing the transformed pairs."
  ^JavaPairRDD
  [f ^JavaPairRDD rdd]
  (rdd/set-callsite-name
    (.mapValues rdd (f/fn1 f))
    (name/fn-name f)))


(defn mapcat-values
  "Map the function `f` over each value of the pairs in `rdd` to produce a
  collection of values. Returns a new pair RDD representing the concatenated
  keys and values."
  ^JavaPairRDD
  [f ^JavaPairRDD rdd]
  (rdd/set-callsite-name
    (.flatMapValues rdd (f/fn1 f))
    (name/fn-name f)))


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
  (rdd/set-callsite-name (.zipWithIndex rdd)))


(defn zip-unique-ids
  "Zip the elements in `rdd` with unique long identifiers. Returns a new pair
  RDD with the element/id tuples.

  Items in the kth partition will get ids `k`, `n+k`, `2*n+k`, ..., where `n`
  is the number of partitions. So the ids won't be sequential and there may be
  gaps, but this method _won't_ trigger a spark job, unlike `zip-indexed`."
  ^JavaPairRDD
  [^JavaRDD rdd]
  (rdd/set-callsite-name (.zipWithUniqueId rdd)))



;; ## Pair RDD Aggregation

(defn group-by
  "Group the elements of `rdd` using a key function `f`. Returns a pair RDD
  with each generated key and all matching elements as a value sequence."
  ^JavaPairRDD
  ([f ^JavaRDD rdd]
   (rdd/set-callsite-name
     (.groupBy rdd (f/fn1 f))
     (name/fn-name f)))
  ^JavaPairRDD
  ([f num-partitions ^JavaRDD rdd]
   (rdd/set-callsite-name
     (.groupBy rdd (f/fn1 f) (int num-partitions))
     (name/fn-name f)
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
    (name/fn-name f)))


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
     (name/fn-name seq-fn)
     (name/fn-name conj-fn)
     (name/fn-name merge-fn)))
  ^JavaPairRDD
  ([seq-fn conj-fn merge-fn num-partitions ^JavaPairRDD rdd]
   (rdd/set-callsite-name
     (.combineByKey rdd
                    (f/fn1 seq-fn)
                    (f/fn2 conj-fn)
                    (f/fn2 merge-fn)
                    (int num-partitions))
     (name/fn-name seq-fn)
     (name/fn-name conj-fn)
     (name/fn-name merge-fn)
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
     (name/fn-name compare-fn)
     (boolean ascending?)))
  ^JavaPairRDD
  ([compare-fn ascending? num-partitions ^JavaPairRDD rdd]
   (rdd/set-callsite-name
     (.sortByKey rdd
                 (f/comparator-fn compare-fn)
                 (boolean ascending?)
                 (int num-partitions))
     (name/fn-name compare-fn)
     (boolean ascending?)
     (int num-partitions))))



;; ## RDD Aggregation Actions

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



;; ## RDD Partitioning

(defn coalesce
  "Decrease the number of partitions in `rdd` to `n`. Useful for running
  operations more efficiently after filtering down a large dataset."
  ^JavaRDD
  ([n ^JavaRDD rdd]
   (coalesce n false rdd))
  ([n shuffle? ^JavaRDD rdd]
   (rdd/set-callsite-name
     (.coalesce rdd (int n) (boolean shuffle?))
     (int n)
     (boolean shuffle?))))



;; ## RDD Actions

(defn foreach
  "Apply the function `f` to all elements of `rdd`. The function will execute
  on the executors where the data resides.

  Consider `foreach-partition` for efficiency if handling an element requires
  costly resource acquisition such as a database connection."
  [f ^JavaRDD rdd]
  (.foreach rdd (f/void-fn f)))
