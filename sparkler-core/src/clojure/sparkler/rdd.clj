(ns sparkler.rdd
  "This namespace provides the main API for writing Spark tasks.

  Most operations in this namespace place the RDD last in the argument list,
  just like Clojure collection functions. This lets you compose them using the
  thread-last macro (`->>`), making it simple to migrate existing Clojure
  code."
  (:require
    [clojure.string :as str]
    [sparkler.name :as name])
  (:import
    (org.apache.spark.api.java
      JavaPairRDD
      JavaRDD
      JavaSparkContext
      StorageLevels)))


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



;; ## RDD Construction

(defn set-callsite-name
  "Provide a name for the given RDD by looking at the current stack. Returns
  the updated RDD if the name could be determined."
  ^JavaRDD
  [^JavaRDD rdd & args]
  (try
    (let [rdd-name (format "#<%s: %s %s>"
                           (.getSimpleName (class rdd))
                           (name/callsite-name)
                           (if (seq args)
                             (str " [" (str/join ", " args) "]")
                             ""))]
      (.setName rdd rdd-name))
    (catch Exception e
      ;; Ignore errors and return an unnamed RDD.
      rdd)))


(defn parallelize
  "Distribute a local collection to form an RDD. Optionally accepts a number
  of partitions to slice the collection into."
  ([^JavaSparkContext spark-context coll]
   (set-callsite-name
     (.parallelize spark-context coll)))
  ([^JavaSparkContext spark-context min-partitions coll]
   (set-callsite-name
     (.parallelize spark-context coll min-partitions)
     min-partitions)))


(defn parallelize-pairs
  "Distributes a local collection to form a pair RDD. Optionally accepts a
  number of partitions to slice the collection into."
  ([^JavaSparkContext spark-context coll]
   (set-callsite-name
     (.parallelizePairs spark-context coll)))
  ([^JavaSparkContext spark-context min-partitions coll]
   (set-callsite-name
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
