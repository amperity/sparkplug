(ns sparkplug.repl.mor-rdd
  ""
  (:gen-class
    :name sparkplug.repl.mor-rdd.MorRDD
    :state state
    :init init
    :main false
    :extends org.apache.spark.rdd.RDD
    :constructors {[org.apache.spark.SparkContext
                    java.lang.Object]
                   [org.apache.spark.SparkContext
                    scala.collection.Seq
                    scala.reflect.ClassTag]})
  (:require
    [clojure.tools.logging :as log]
    [sparkplug.scala :as scala])
  (:import
    clojure.lang.ILookup
    (org.apache.spark
      Partition
      TaskContext
      OneToOneDependency)
    (org.apache.spark.api.java
      JavaPairRDD
      JavaSparkContext)
    org.apache.spark.rdd.RDD
    scala.collection.JavaConversions
    scala.collection.mutable.ArrayBuffer
    scala.reflect.ClassManifestFactory$
    (sparkplug.repl.mor-rdd MorRDD)))



;; ## Partition Scan

(defn- class-tag
  "Generates a Scala `ClassTag` for the given class."
  [^Class cls]
  (.fromClass ClassManifestFactory$/MODULE$ cls))



;; ## RDD Methods

(defn ^:no-doc -init
  [spark-ctx rdds]
  (let [deps (->> rdds
                  (map #(OneToOneDependency. (.rdd ^JavaPairRDD %)))
                  (.iterator)
                  (JavaConversions/asScalaIterator))]
    [[spark-ctx
      (doto (ArrayBuffer.) (.appendAll deps))
      (class-tag Object)]
     {:parents rdds}]))


(defn ^:no-doc -partitioner
  [this]
  (.partitioner (first (:parents (.state this)))))


(defn ^:no-doc -getPartitions
  [this]
  (into-array Partition (.partitions (first (:parents (.state this))))))


(defn- merge-seqs
  [s1 s2]
  (println "merge-seqs:" s1 s2)
  (let [first-key #(scala/first (first %))]
    (lazy-seq
      (cond
        (empty? s1)
        s2

        (empty? s2)
        s1

        ;; Next key is in both parts
        (= (first-key s1) (first-key s2))
        ;; TODO: Handle merge more realistically. For now just always pick the row from s1.
        (cons (first s1) (merge-seqs (rest s1) (rest s2)))

        ;; Next key is in s1, not in s2.
        (neg? (compare (first-key s1) (first-key s2)))
        (cons (first s1) (merge-seqs (rest s1) s2))

        ;; Next key is in s2, not in s1.
        ;; (pos? (compare (scala/first (first s1)) (scala/first (first s2))))
        :else
        (cons (first s2) (merge-seqs s1 (rest s2)))))))


(defn ^:no-doc -compute
  [this part ^TaskContext task-context]
  (let [parents (:parents (.state this))]
    ;; TODO: Generalize to N input RDDs
    (when (not= 2 (count parents)) (throw (ex-info "WIP: Only supports two input RDDs" {})))
    (->>
      parents
      (map #(.compute (.rdd %) part task-context))
      (map #(JavaConversions/asJavaIterator %))
      (map iterator-seq)
      (apply merge-seqs)
      (.iterator)
      (JavaConversions/asScalaIterator))))



;; ## RDD Construction

;; TODO: Validate that all input RDDs:
;;
;; * Have the same Partitioner
;; * Have the same number of partitions
;; * Are JavaPairRDDs
(defn build
  [spark-ctx parents]
  ;; FIXME: private fn access
  (let [sc (.sc ^JavaSparkContext spark-ctx)]
    (-> (MorRDD.
          (.sc ^JavaSparkContext spark-ctx)
          parents)
        (JavaPairRDD/fromRDD
          (class-tag Object)
          (class-tag Object))
        ;; TODO: Populate name more usefully.
        (.setName "MorRDD"))))
