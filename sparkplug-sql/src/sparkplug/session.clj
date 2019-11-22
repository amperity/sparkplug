(ns sparkplug.session
  "Functions for working with and creating Spark sessions."
  (:import
    org.apache.spark.api.java.JavaSparkContext
    org.apache.spark.SparkConf
    org.apache.spark.sql.SparkSession))


#_(defn spark-session
  "Create a new spark session which takes its settings from the given
  configuration object."
  ^SparkSession
  [^SparkConf conf & {:keys [hive-support?]}]
  (-> (SparkSession/builder)
      (.config conf)
      (cond-> hive-support?
              (.enableHiveSupport))
      (.getOrCreate)))


#_(defn ->context
  "Extracts the spark context from the given spark session."
  ^JavaSparkContext
  [^SparkSession session]
  (JavaSparkContext/fromSparkContext
    (.sparkContext session)))
