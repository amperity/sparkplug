(ns sparkplug.rdd
  "This namespace provides the main API for writing Spark tasks.

  Most operations in this namespace place the RDD last in the argument list,
  just like Clojure collection functions. This lets you compose them using the
  thread-last macro (`->>`), making it simple to migrate existing Clojure
  code."
  (:refer-clojure :exclude [empty name partition-by])
  (:require
    [clojure.string :as str]
    [sparkplug.function :as f]
    [sparkplug.scala :as scala])
  (:import
    clojure.lang.Compiler
    (org.apache.spark
      HashPartitioner
      Partitioner)
    (org.apache.spark.api.java
      JavaPairRDD
      JavaRDD
      JavaRDDLike
      JavaSparkContext
      StorageLevels)
    sparkplug.partition.FnHashPartitioner))


;; ## Naming Functions

;; Type hints are omitted because `name` is not included in JavaRDDLike.
(defn name
  "Return the current name for `rdd`."
  [rdd]
  (.name rdd))


;; Type hints are omitted because `setName` is not included in JavaRDDLike.
(defn set-name
  "Set the name of `rdd` to `name-str`."
  ^JavaRDDLike
  [name-str rdd]
  (.setName rdd name-str))


(defn- internal-call?
  "True if a stack-trace element should be ignored because it represents an internal
  function call that should not be considered a callsite."
  [^StackTraceElement element]
  (let [class-name (.getClassName element)]
    (or (str/starts-with? class-name "sparkplug.")
        (str/starts-with? class-name "clojure.lang."))))


(defn- stack-callsite
  "Find the top element in the current stack trace that is not an internal
  function call."
  ^StackTraceElement
  []
  (first (remove internal-call? (.getStackTrace (Exception.)))))


(defn ^:no-doc fn-name
  "Return the (unmangled) name of the given Clojure function."
  [f]
  (Compiler/demunge (.getName (class f))))


(defn- callsite-name
  "Generate a name for the callsite of this function by looking at the current
  stack. Ignores core Clojure and internal function frames."
  []
  (let [callsite (stack-callsite)
        filename (.getFileName callsite)
        classname (.getClassName callsite)
        line-number (.getLineNumber callsite)]
    (format "%s %s:%d" (Compiler/demunge classname) filename line-number)))


(defn ^:no-doc set-callsite-name
  "Provide a name for the given RDD by looking at the current stack. Returns
  the updated RDD if the name could be determined."
  ^JavaRDD
  [^JavaRDD rdd & args]
  (try
    (let [rdd-name (format "#<%s: %s %s>"
                           (.getSimpleName (class rdd))
                           (callsite-name)
                           (if (seq args)
                             (str " [" (str/join ", " args) "]")
                             ""))]
      (.setName rdd rdd-name))
    (catch Exception e
      ;; Ignore errors and return an unnamed RDD.
      rdd)))



;; ## Dataset Construction

(defn empty
  "Construct a new empty RDD."
  ^JavaRDD
  ([^JavaSparkContext spark-context]
   (.emptyRDD spark-context)))


(defn parallelize
  "Distribute a local collection to form an RDD. Optionally accepts a number
  of partitions to slice the collection into."
  ^JavaRDD
  ([^JavaSparkContext spark-context coll]
   (set-callsite-name
     (.parallelize spark-context coll)))
  ^JavaRDD
  ([^JavaSparkContext spark-context min-partitions coll]
   (set-callsite-name
     (.parallelize spark-context coll min-partitions)
     min-partitions)))


(defn parallelize-pairs
  "Distributes a local collection to form a pair RDD. Optionally accepts a
  number of partitions to slice the collection into."
  ^JavaPairRDD
  ([^JavaSparkContext spark-context coll]
   (set-callsite-name
     (.parallelizePairs
       spark-context
       (map scala/to-pair coll))))
  ^JavaPairRDD
  ([^JavaSparkContext spark-context min-partitions coll]
   (set-callsite-name
     (.parallelizePairs
       spark-context
       (map scala/to-pair coll)
       min-partitions)
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
  [path ^JavaRDDLike rdd]
  (.saveAsTextFile rdd (str path)))



;; ## Partitioning Logic

(defn hash-partitioner
  "Construct a partitioner which will hash keys to distribute them uniformly
  over `n` buckets. Optionally accepts a `key-fn` which will be called on each
  key before hashing it."
  ^Partitioner
  ([n]
   (HashPartitioner. n))
  ^Partitioner
  ([key-fn n]
   (FnHashPartitioner. (int n) (f/fn1 key-fn))))


(defn partitions
  "Return a vector of the partitions in `rdd`."
  [^JavaRDDLike rdd]
  (into [] (.partitions (.rdd rdd))))


(defn num-partitions
  "Returns the number of partitions in `rdd`."
  [^JavaRDDLike rdd]
  (.getNumPartitions rdd))


(defn partitioner
  "Return the partitioner associated with `rdd`, or nil if there is no custom
  partitioner."
  [^JavaRDDLike rdd]
  (scala/resolve-option
    (.partitioner (.rdd rdd))))


(defn partition-by
  "Return a copy of `rdd` partitioned by the given `partitioner`."
  [^Partitioner partitioner ^JavaPairRDD rdd]
  (set-callsite-name
    (.partitionBy rdd partitioner)
    (.getName (class partitioner))))


;; Type hints are omitted because `repartition` is not included in JavaRDDLike.
(defn repartition
  "Returns a new `rdd` with exactly `n` partitions.

  This method can increase or decrease the level of parallelism in this RDD.
  Internally, this uses a shuffle to redistribute data.

  If you are decreasing the number of partitions in this RDD, consider using
  `coalesce`, which can avoid performing a shuffle."
  ^JavaRDDLike
  [n rdd]
  (set-callsite-name
    (.repartition rdd (int n))
    (int n)))


(defn repartition-and-sort-within-partitions
  "Repartition the RDD according to the given partitioner and, within each
  resulting partition, sort records by their keys. This is more efficient than
  calling repartition and then sorting within each partition because it can
  push the sorting down into the shuffle machinery."
  ^JavaPairRDD
  ([^Partitioner partitioner ^JavaPairRDD pair-rdd]
   (.repartitionAndSortWithinPartitions pair-rdd partitioner))
  ([^Partitioner partitioner ^java.util.Comparator comparator ^JavaPairRDD pair-rdd]
   (.repartitionAndSortWithinPartitions pair-rdd partitioner comparator)))


;; Type hints are omitted because `coalesce` is not included in JavaRDDLike.
(defn coalesce
  "Decrease the number of partitions in `rdd` to `n`. Useful for running
  operations more efficiently after filtering down a large dataset."
  ([num-partitions rdd]
   (coalesce num-partitions false rdd))
  ([num-partitions shuffle? rdd]
   (set-callsite-name
     (.coalesce rdd (int num-partitions) (boolean shuffle?))
     (int num-partitions)
     (boolean shuffle?))))



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


(defn storage-level
  "Return the keyword representing the storage level in the `storage-levels`
  map, or the raw value if not found."
  [^JavaRDD rdd]
  (let [level (.getStorageLevel rdd)]
    (or (->> storage-levels
             (filter #(= level (val %)))
             (map key)
             (first))
        level)))


;; Type hints are omitted because `cache` and `persist` are not included in
;; JavaRDDLike.
(defn cache!
  "Sets the storage level of `rdd` to persist its values across operations
  after the first time it is computed. By default, this uses the `:memory-only`
  level, but an alternate may be specified by `level`.

  This can only be used to assign a new storage level if the RDD does not have
  a storage level set already."
  ([rdd]
   (.cache rdd))
  ([level rdd]
   {:pre [(contains? storage-levels level)]}
   (.persist rdd (get storage-levels level))))


;; Type hints are omitted because `unpersist` is not included in JavaRDDLike.
(defn uncache!
  "Mark `rdd` as non-persistent, and remove all blocks for it from memory and
  disk. Blocks until all data has been removed unless `blocking?` is provided
  and false."
  ([rdd]
   (.unpersist rdd))
  ([blocking? rdd]
   (.unpersist rdd (boolean blocking?))))


(defn checkpointed?
  "True if `rdd` has been marked for checkpointing."
  [^JavaRDDLike rdd]
  (.isCheckpointed rdd))


(defn checkpoint!
  "Mark `rdd` for checkpointing. It will be saved to a file inside the
  checkpoint directory set on the Spark context and all references to its
  parent RDDs will be removed.

  This function must be called before any job has been executed on this RDD. It
  is strongly recommended that this RDD is persisted in memory, otherwise
  saving it to a file will require recomputation."
  [^JavaRDDLike rdd]
  (.checkpoint rdd))
