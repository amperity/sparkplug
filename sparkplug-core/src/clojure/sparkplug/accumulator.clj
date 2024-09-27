(ns sparkplug.accumulator
  "Functions for working with Accumulator objects which can aggregate values
  across executors."
  (:refer-clojure :exclude [count empty? name reset!])
  (:require
    [sparkplug.scala :as scala])
  (:import
    org.apache.spark.api.java.JavaSparkContext
    (org.apache.spark.util
      AccumulatorV2
      DoubleAccumulator
      LongAccumulator)))


;; ## Constructors

(defn long-accumulator
  "Create and register a long accumulator, which starts with 0 and accumulates
  inputs by summing them."
  ([^JavaSparkContext spark-context]
   (.longAccumulator (.sc spark-context)))
  ([^JavaSparkContext spark-context acc-name]
   (.longAccumulator (.sc spark-context) acc-name)))


(defn double-accumulator
  "Create and register a double accumulator, which starts with 0.0 and
  accumulates inputs by summing them."
  ([^JavaSparkContext spark-context]
   (.doubleAccumulator (.sc spark-context)))
  ([^JavaSparkContext spark-context acc-name]
   (.doubleAccumulator (.sc spark-context) acc-name)))


(defn collection-accumulator
  "Create and register a collection accumulator, which starts with empty list
  and accumulates inputs by adding them into the list."
  ([^JavaSparkContext spark-context]
   (.collectionAccumulator (.sc spark-context)))
  ([^JavaSparkContext spark-context acc-name]
   (.collectionAccumulator (.sc spark-context) acc-name)))


;; ## Accumulator Methods

(defn name
  "Return the name of the accumulator, if any."
  [^AccumulatorV2 acc]
  (scala/resolve-option (.name acc)))


(defn value
  "Return the current value of the accumulator. This can only be called by the
  driver."
  [^AccumulatorV2 acc]
  (.value acc))


(defn empty?
  "True if the accumulator has not had any values added to it."
  [^AccumulatorV2 acc]
  (.isZero acc))


(defn add!
  "Add an element to the accumulated value."
  [^AccumulatorV2 acc v]
  (.add acc v))


(defn merge!
  "Merge an accumulator `b` into `a`. Both accumulators must have the same
  type."
  [^AccumulatorV2 a ^AccumulatorV2 b]
  (.merge a b))


(defn reset!
  "Reset the accumulator to its empty or zero value."
  [^AccumulatorV2 acc]
  (.reset acc))


;; ## Numeric Accumulators

(defn count
  "Return the number of values added to the accumulator. The accumulator must
  hold either long or double values."
  [acc]
  (condp instance? acc
    LongAccumulator
    (.count ^LongAccumulator acc)

    DoubleAccumulator
    (.count ^DoubleAccumulator acc)

    (throw (IllegalArgumentException.
             (str "Cannot call count on accumulator type "
                  (class acc))))))


(defn sum
  "Return the sum of all the values added to the accumulator. The accumulator
  must hold either long or double values."
  [acc]
  (condp instance? acc
    LongAccumulator
    (.sum ^LongAccumulator acc)

    DoubleAccumulator
    (.sum ^DoubleAccumulator acc)

    (throw (IllegalArgumentException.
             (str "Cannot call sum on accumulator type "
                  (class acc))))))


(defn avg
  "Return the average of all the values added to the accumulator. The
  accumulator must hold either long or double values."
  [acc]
  (condp instance? acc
    LongAccumulator
    (.avg ^LongAccumulator acc)

    DoubleAccumulator
    (.avg ^DoubleAccumulator acc)

    (throw (IllegalArgumentException.
             (str "Cannot call avg on accumulator type "
                  (class acc))))))
