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
    [sparkler.name :as name])
  (:import
    (org.apache.spark.api.java
      JavaPairRDD
      JavaRDD
      JavaSparkContext
      StorageLevels)))


;; ## Constants

(def storage-levels
  "Keyword mappings for available RDD storage levels."
  {:memory-only           StorageLevels/MEMORY_ONLY
   :memory-only-ser       StorageLevels/MEMORY_ONLY_SER
   :memory-and-disk       StorageLevels/MEMORY_AND_DISK
   :memory-and-disk-ser   StorageLevels/MEMORY_AND_DISK_SER
   :disk-only             StorageLevels/DISK_ONLY
   :memory-only-2         StorageLevels/MEMORY_ONLY_2
   :memory-only-ser-2     StorageLevels/MEMORY_ONLY_SER_2
   :memory-and-disk-2     StorageLevels/MEMORY_AND_DISK_2
   :memory-and-disk-ser-2 StorageLevels/MEMORY_AND_DISK_SER_2
   :disk-only-2           StorageLevels/DISK_ONLY_2
   :none                  StorageLevels/NONE})



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



;; ## RDD Construction

;; TODO: move to `sparkler.rdd`?

(defn parallelize
  "Distribute a local collection to form an RDD. Optionally accepts a number
  of partitions to slice the collection into."
  ([^JavaSparkContext spark-context coll]
   (name/set-callsite-name
     (.parallelize spark-context coll)))
  ([^JavaSparkContext spark-context min-partitions coll]
   (name/set-callsite-name
     (.parallelize spark-context coll min-partitions)
     min-partitions)))


(defn parallelize-pairs
  "Distributes a local collection to form a pair RDD. Optionally accepts a
  number of partitions to slice the collection into."
  ([^JavaSparkContext spark-context coll]
   (name/set-callsite-name
     (.parallelizePairs spark-context coll)))
  ([^JavaSparkContext spark-context min-partitions coll]
   (name/set-callsite-name
     (.parallelizePairs spark-context coll min-partitions)
     min-partitions)))


(defn text-file
  "Read a text file from a URL into an RDD of the lines in the file. Optionally
  accepts a number of partitions to slice the file into."
  ([^JavaSparkContext spark-context filename]
   (.textFile spark-context filename))
  ([^JavaSparkContext spark-context min-partitions filename]
   (.textFile spark-context filename min-partitions)))


(defn whole-text-files
  "Read a directory of text files from a URL into an RDD. Each element of the
  RDD is a pair of the file path and the full contents of the file."
  ([^JavaSparkContext spark-context filename]
   (.wholeTextFiles spark-context filename))
  ([^JavaSparkContext spark-context min-partitions filename]
   (.wholeTextFiles spark-context filename min-partitions)))



;; ## RDD Transformations

(defn filter
  "Filter the elements of `rdd` to the ones which satisfy the predicate `f`."
  ^JavaRDD
  [f ^JavaRDD rdd]
  (name/set-callsite-name
    (.filter rdd (f/fn1 (comp boolean f)))
    (name/fn-name f)))


(defn map
  "Map the function `f` over each element of `rdd`. Returns a new RDD
  representing the transformed elements."
  ^JavaRDD
  [f ^JavaRDD rdd]
  (name/set-callsite-name
    (.map rdd (f/fn1 f))
    (name/fn-name f)))


(defn mapcat
  "Map the function `f` over each element in `rdd` to produce a sequence of
  results. Returns an RDD representing the concatenation of all element
  results."
  ^JavaRDD
  [f ^JavaRDD rdd]
  (name/set-callsite-name
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
   (name/set-callsite-name
     (.mapPartitions rdd (f/flat-map-fn f))
     (name/fn-name f))))


(defn map-partitions-indexed
  "Map the function `f` over each partition in `rdd`, producing a sequence of
  results. Returns an RDD representing the concatenation of all the partition
  results. The function will be called with the partition index and an iterator
  of the elements of each partition."
  ^JavaRDD
  [f ^JavaRDD rdd]
  (name/set-callsite-name
    (.mapPartitionsWithIndex rdd (f/fn2 f) true)
    (name/fn-name f)))



;; ## Pair RDD Transformations

(defn keys
  "Transform `rdd` by replacing each pair with its key. Returns a new RDD
  representing the keys."
  ^JavaRDD
  [^JavaPairRDD rdd]
  (name/set-callsite-name (.keys rdd)))


(defn values
  "Transform `rdd` by replacing each pair with its value. Returns a new RDD
  representing the values."
  ^JavaRDD
  [^JavaPairRDD rdd]
  (name/set-callsite-name (.values rdd)))


(defn key-by
  "Creates pairs from the elements in `rdd` by using `f` to compute a key for
  each value."
  [f ^JavaRDD rdd]
  (name/set-callsite-name
    (.mapToPair rdd (f/pair-fn (juxt f identity)))
    (name/fn-name f)))


(defn map->pairs
  "Map the function `f` over each element of `rdd`. Returns a new pair RDD
  representing the transformed elements."
  ^JavaPairRDD
  [f ^JavaRDD rdd]
  (name/set-callsite-name
    (.mapToPair rdd (f/pair-fn f))
    (name/fn-name f)))


(defn mapcat->pairs
  "Map the function `f` over each element in `rdd` to produce a sequence of
  key-value pairs. Returns a new pair RDD representing the concatenation of all
  result pairs."
  ^JavaPairRDD
  [f ^JavaRDD rdd]
  (name/set-callsite-name
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
   (name/set-callsite-name
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
  (name/set-callsite-name
    (.mapValues rdd (f/fn1 f))
    (name/fn-name f)))


(defn mapcat-values
  "Map the function `f` over each value of the pairs in `rdd` to produce a
  collection of values. Returns a new pair RDD representing the concatenated
  keys and values."
  ^JavaPairRDD
  [f ^JavaPairRDD rdd]
  (name/set-callsite-name
    (.flatMapValues rdd (f/fn1 f))
    (name/fn-name f)))



;; ## RDD Aggregations

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



;; ## RDD Actions

(defn foreach
  "Apply the function `f` to all elements of `rdd`. The function will execute
  on the executors where the data resides.

  Consider `foreach-partition` for efficiency if handling an element requires
  costly resource acquisition such as a database connection."
  [f ^JavaRDD rdd]
  (.foreach rdd (f/void-fn f)))
