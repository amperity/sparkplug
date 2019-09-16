(ns sparkplug.config
  "Functions for working with Spark configuration."
  (:import
    org.apache.spark.SparkConf))


;; ## Constructor

(defn spark-conf
  "Construct a new Spark configuration. Optionally accepts a boolean to control
  whether default configuration is loaded from the system properties."
  ^SparkConf
  ([]
   (spark-conf true))
  ^SparkConf
  ([defaults?]
   (-> (SparkConf. (boolean defaults?))
       (.set "spark.serializer" "org.apache.spark.serializer.KryoSerializer")
       (.set "spark.kryo.registrator" "sparkplug.kryo.Registrator"))))



;; ## Property Accessors

(defn contains-key?
  "True if the given spark configuration contains the named parameter."
  [^SparkConf conf ^String k]
  (.contains conf k))


(defn get-all
  "Get all configuration parameters as a map."
  [^SparkConf conf]
  (into {}
        (map (fn tuple->entry
               [^scala.Tuple2 entry]
               [(._1 entry) (._2 entry)]))
        (.getAll conf)))


(defn get-param
  "Get a configuration parameter `k` in `conf`. If not set, this throws a
  `NoSuchElementException` or returns `not-found` if provided."
  ([^SparkConf conf ^String k]
   (.get conf k))
  ([^SparkConf conf ^String k ^String not-found]
   (.get conf k not-found)))


(defn merge-params
  "Merge the provided parameters into the Spark configuration. Returns updated
  configuration."
  ^SparkConf
  [^SparkConf conf params]
  (reduce-kv
    (fn set-entry
      [^SparkConf c ^String k ^String v]
      (.set c k v))
    conf
    params))


(defn set-param
  "Set a parameter to a new value in the given Spark configuration. Returns
  updated configuration."
  ^SparkConf
  ([^SparkConf conf ^String k ^String v]
   (.set conf k v))
  ^SparkConf
  ([^SparkConf conf k v & kvs]
   {:pre [(even? (count kvs))]}
   (merge-params conf (apply array-map k v kvs))))


(defn set-param-default
  "Set a parameter to a new value if it is not already set in the config.
  Returns an updated configuration."
  ^SparkConf
  [^SparkConf conf ^String k ^String v]
  (.setIfMissing conf k v))


(defn unset-param
  "Unset the given parameters on the config. Returns an updated config."
  ^SparkConf
  ([^SparkConf conf ^String k]
   (.remove conf k))
  ^SparkConf
  ([^SparkConf conf k & ks]
   (reduce
     (fn unset-key
       [^SparkConf c ^String k]
       (.remove c k))
     conf
     (cons k ks))))


(defn set-executor-env
  "Set environment variables to be used when launching executors for this
  application. Accepts a parameter key and value or a map of parameters.
  Returns an updated configuration."
  ^SparkConf
  ([^SparkConf conf k v]
   (.setExecutorEnv conf k v))
  ^SparkConf
  ([^SparkConf conf env]
   (reduce-kv
     (fn set-entry
       [^SparkConf c k v]
       (.setExecutorEnv c k v))
     conf
     env)))


(defn master
  "Set the Spark master property. Returns updated configuration."
  ^SparkConf
  [^SparkConf conf ^String master]
  (.setMaster conf master))


(defn spark-home
  "Set the Spark home path property. Returns updated configuration."
  ^SparkConf
  [^SparkConf conf home]
  (.setSparkHome conf home))


(defn app-name
  "Set the Spark application name. Returns updated configuration."
  ^SparkConf
  [^SparkConf conf name-str]
  (.setAppName conf name-str))


(defn jars
  "Set JAR files to distribute to the cluster. Returns updated configuration."
  ^SparkConf
  [^SparkConf conf jars]
  (.setJars conf ^"[Ljava.lang.String;" (into-array String jars)))


(defn debug-str
  "Return a string containing a representation of the configuration useful for
  debugging."
  [^SparkConf conf]
  (.toDebugString conf))
