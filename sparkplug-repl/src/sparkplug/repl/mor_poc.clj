(ns sparkplug.repl.mor-poc
  (:require
    [amperity.common.util :as util]
    [sparkplug.scala :as scala]
    [sparkplug.rdd :as rdd]
    [sparkplug.core :as spark])
  (:import
    (org.apache.spark.sql
      Row
      RowFactory)
    (org.apache.spark.sql.types
      DataTypes
      Metadata
      StructField
      StructType)
    (scala Tuple2)
    (org.apache.spark.sql.hive HiveContext)
    (org.apache.spark.api.java JavaRDDLike)))


(defn- make-str
  [len index]
  (format (str "%0" len "d") index))


(defn make-row
  [str-len str-count int-count index]
  (let [make-str' (partial make-str str-len)]
    (merge
      {:pk (make-str' index)}
      (zipmap
        (map #(keyword (format "str%02d" %)) (range str-count))
        (repeat (make-str' index)))
      (zipmap
        (map #(keyword (format "int%02d" %)) (range int-count))
        (repeat index)))))


(defn make-data
  ([row-count]
   (make-data 8 10 10 row-count))
  ([str-len str-count int-count row-count]
   (->> (range)
        (map (partial make-row str-len str-count int-count))
        (take row-count))))


(defn- struct-field
  [type name]
  (StructField. name type true (Metadata/empty)))


(defn make-schema
  [str-count int-count]
  (StructType.
    (into-array
      StructField
      (concat
        [(struct-field DataTypes/StringType "pk")]
        (->> (range str-count)
             (map #(format "str%02d" %))
             (map (partial struct-field DataTypes/StringType)))
        (->> (range int-count)
             (map #(format "int%02d" %))
             (map (partial struct-field DataTypes/LongType)))))))


(defn- get-field-value
  [m field]
  (get m (keyword (.name field))))


(defn tuple2->row
  [fields ^Tuple2 tuple]
  (let [m (scala/second tuple)]
    (RowFactory/create
      (into-array Object (map (partial get-field-value m) fields)))))


(defn partition-and-sort
  ([rdd]
   (partition-and-sort 2 rdd))
  ([n rdd]
   (->> rdd
        (spark/key-by (comp hash :pk))
        ;; TODO: providing key-fn seems to break due to java.io.NotSerializableException,
        ;; presumably because of the `proxy` call in `rdd/hash-partitioner` impl. See this error:
        ;;
        ;; Job aborted due to stage failure: Task not serializable: java.io.NotSerializableException:
        ;; sparkplug.rdd.proxy$org.apache.spark.HashPartitioner$ff19274a
        (rdd/partition-by (rdd/hash-partitioner #_identity n))
        (spark/reduce-by-key merge)
        (spark/map-partitions (comp (partial sort-by scala/first) iterator-seq) true))))


(defn rows->df
  [sc schema partitions rows]
  (let [rdd (->> rows
                 (rdd/parallelize sc)
                 (partition-and-sort partitions)
                 (spark/map (partial tuple2->row (.fields schema))))]
    (.createDataFrame (HiveContext. sc) rdd schema)))


(defn write-df!
  [df path]
  (.. df (write) (format "orc") (save path)))
