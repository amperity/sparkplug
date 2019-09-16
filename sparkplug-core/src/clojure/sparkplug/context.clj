(ns sparkplug.context
  "Functions for working with and creating Spark contexts."
  (:require
    [sparkplug.util :as u])
  (:import
    org.apache.spark.SparkConf
    org.apache.spark.api.java.JavaSparkContext))


;; ## Application Lifecycle

(defn spark-context
  "Create a new spark context which takes its settings from the given
  configuration object."
  ^JavaSparkContext
  ([^SparkConf conf]
   (JavaSparkContext. conf))
  ^JavaSparkContext
  ([master app-name]
   (JavaSparkContext. (str master) (str app-name))))


(defn set-job-description!
  "Set a human readable description of the current job."
  [^JavaSparkContext spark-context description]
  (.setJobDescription spark-context description))


(defn set-job-group!
  "Assign a group ID to all the jobs started by this thread until the group ID
  is set to a different value or cleared.

  Often, a unit of execution in an application consists of multiple Spark
  actions or jobs. Application programmers can use this method to group all
  those jobs together and give a group description. Once set, the Spark web UI
  will associate such jobs with this group.

  The application can later use `cancel-job-group!` to cancel all running jobs
  in this group. If `interrupt?` is set to true for the job group, then job
  cancellation will result in the job's executor threads being interrupted."
  ([^JavaSparkContext spark-context group-id description]
   (.setJobGroup spark-context group-id description))
  ([^JavaSparkContext spark-context group-id description interrupt?]
   (.setJobGroup spark-context group-id description (boolean interrupt?))))


(defn clear-job-group!
  "Clear the current thread's job group ID and its description."
  [^JavaSparkContext spark-context]
  (.clearJobGroup spark-context))


(defn cancel-job-group!
  "Cancel active jobs for the specified group.

  See `set-job-group!` for more information."
  [^JavaSparkContext spark-context group-id]
  (.cancelJobGroup spark-context group-id))


(defn cancel-all-jobs!
  "Cancel all jobs that have been scheduled or are running."
  [^JavaSparkContext spark-context]
  (.cancelAllJobs spark-context))


(defn stop!
  "Shut down the Spark context."
  [^JavaSparkContext spark-context]
  (.stop spark-context))


(defmacro with-context
  "Evaluate `body` within a new Spark context by constructing one from the
  given expression. The context is stopped after evaluation is complete."
  [binding-vec & body]
  {:pre [(vector? binding-vec) (= 2 (count binding-vec))]}
  (let [[ctx-sym expr] binding-vec
        ctx-sym (vary-meta ctx-sym assoc :tag 'org.apache.spark.api.java.JavaSparkContext)]
    `(let [~ctx-sym (spark-context ~expr)]
       (try
         ~@body
         (finally
           (stop! ~ctx-sym))))))



;; ## Context Introspection

(defn config
  "Return the Spark configuration used for the given context."
  ^SparkConf
  [^JavaSparkContext spark-context]
  (.getConf spark-context))


(defn info
  "Build a map of information about the Spark context."
  [^JavaSparkContext spark-context]
  {:master (.master spark-context)
   :app-name (.appName spark-context)
   :local? (.isLocal spark-context)
   :user (.sparkUser spark-context)
   :start-time (.startTime spark-context)
   :version (.version spark-context)
   :jars (.jars spark-context)
   :default-min-partitions (.defaultMinPartitions spark-context)
   :default-parallelism (.defaultParallelism spark-context)
   :checkpoint-dir (u/resolve-option (.getCheckpointDir spark-context))})


(defn get-local-property
  "Get a local property set for this thread, or null if not set."
  [^JavaSparkContext spark-context k]
  (.getLocalProperty spark-context k))


(defn persistent-rdds
  "Return a Java map of JavaRDDs that have marked themselves as persistent via
  a `cache!` call."
  [^JavaSparkContext spark-context]
  (into {} (.getPersistentRDDs spark-context)))



;; ## Context Modifiers

(defn add-file!
  "Add a file to be downloaded with this Spark job on every node."
  ([^JavaSparkContext spark-context path]
   (.addFile spark-context path))
  ([^JavaSparkContext spark-context path recursive?]
   (.addFile spark-context path (boolean recursive?))))


(defn add-jar!
  "Adds a JAR dependency for all tasks to be executed on this SparkContext in
  the future."
  [^JavaSparkContext spark-context path]
  (.addJar spark-context path))


(defn set-local-property!
  "Set a local property that affects jobs submitted from this thread, and all
  child threads, such as the Spark fair scheduler pool."
  [^JavaSparkContext spark-context k v]
  (.setLocalProperty spark-context k v))


(defn set-checkpoint-dir!
  "Set the directory under which RDDs are going to be checkpointed."
  [^JavaSparkContext spark-context path]
  (.setCheckpointDir spark-context path))


(defn set-log-level!
  "Control the Spark application's logging level."
  [^JavaSparkContext spark-context level]
  (.setLogLevel spark-context level))
