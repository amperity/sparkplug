(ns sparkplug.rdd
  "This namespace provides the main API for writing Spark tasks.

  Most operations in this namespace place the RDD last in the argument list,
  just like Clojure collection functions. This lets you compose them using the
  thread-last macro (`->>`), making it simple to migrate existing Clojure
  code."
  (:refer-clojure :exclude [name])
  (:require
    [clojure.string :as str]
    [sparkplug.name :as name])
  (:import
    (org.apache.spark.api.java
      JavaPairRDD
      JavaRDD
      JavaSparkContext
      StorageLevels)))


;; ## RDD Naming

(defn name
  "Return the current name for `rdd`."
  [^JavaRDD rdd]
  (.name rdd))


(defn set-name
  "Set the name of `rdd` to `name-str`."
  ^JavaRDD
  [name-str ^JavaRDD rdd]
  (.setName rdd name-str))


(defn ^:no-doc set-callsite-name
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



;; ## RDD Construction

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


(defn binary-files
  "Read a directory of binary files from the given URL as a pair RDD of paths
  to byte streams."
  ^JavaPairRDD
  ([^JavaSparkContext spark-context path]
   (.binaryFiles spark-context path))
  ^JavaPairRDD
  ([^JavaSparkContext spark-context path num-partitions]
   (.binaryFiles spark-context path (int num-partitions))))


(defn text-file
  "Read a text file from a URL into an RDD of the lines in the file. Optionally
  accepts a number of partitions to slice the file into."
  ^JavaRDD
  ([^JavaSparkContext spark-context filename]
   (.textFile spark-context filename))
  ^JavaRDD
  ([^JavaSparkContext spark-context min-partitions filename]
   (.textFile spark-context filename min-partitions)))


(defn whole-text-files
  "Read a directory of text files from a URL into an RDD. Each element of the
  RDD is a pair of the file path and the full contents of the file."
  ^JavaPairRDD
  ([^JavaSparkContext spark-context filename]
   (.wholeTextFiles spark-context filename))
  ^JavaPairRDD
  ([^JavaSparkContext spark-context min-partitions filename]
   (.wholeTextFiles spark-context filename min-partitions)))


(defn save-as-text-file
  "Write the elements of `rdd` as a text file (or set of text files) in a given
  directory `path` in the local filesystem, HDFS or any other Hadoop-supported
  file system. Spark will call toString on each element to convert it to a line
  of text in the file."
  [path ^JavaRDD rdd]
  (.saveAsTextFile rdd (str path)))



;; ## Storage Management

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


(defn cache!
  "Sets the storage level of `rdd` to persist its values across operations
  after the first time it is computed. By default, this uses the `:memory-only`
  level, but an alternate may be specified by `level`.

  This can only be used to assign a new storage level if the RDD does not have
  a storage level set already."
  ^JavaRDD
  ([^JavaRDD rdd]
   (.cache rdd))
  ^JavaRDD
  ([level ^JavaRDD rdd]
   {:pre [(contains? storage-levels level)]}
   (.persist rdd (get storage-levels level))))


(defn uncache!
  "Mark `rdd` as non-persistent, and remove all blocks for it from memory and
  disk. Blocks until all data has been removed unless `blocking?` is provided
  and false."
  ^JavaRDD
  ([^JavaRDD rdd]
   (.unpersist rdd))
  ^JavaRDD
  ([blocking? ^JavaRDD rdd]
   (.unpersist rdd (boolean blocking?))))
