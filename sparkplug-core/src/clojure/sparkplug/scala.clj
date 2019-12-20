(ns sparkplug.scala
  "Commonly used utilities for interop with Scala objects."
  (:refer-clojure :exclude [first second])
  (:require
    [clojure.walk :as walk])
  (:import
    clojure.lang.MapEntry
    (scala
      Option
      Product
      Some
      Tuple1
      Tuple2
      Tuple3
      Tuple4
      Tuple5
      Tuple6
      Tuple7
      Tuple8
      Tuple9)))


(defn resolve-option
  "Resolve an optional type to some value or nil."
  [^Option o]
  (when (instance? Some o)
    (.get ^Some o)))



;; ## Tuples

(defn tuple
  "Construct a Scala tuple. Supports tuples up to size 9."
  ([a]
   (Tuple1. a))
  ([a b]
   (Tuple2. a b))
  ([a b c]
   (Tuple3. a b c))
  ([a b c d]
   (Tuple4. a b c d))
  ([a b c d e]
   (Tuple5. a b c d e))
  ([a b c d e f]
   (Tuple6. a b c d e f))
  ([a b c d e f g]
   (Tuple7. a b c d e f g))
  ([a b c d e f g h]
   (Tuple8. a b c d e f g h))
  ([a b c d e f g h i]
   (Tuple9. a b c d e f g h i)))


(defn vec->tuple
  "Coerce a Clojure vector to a Scala tuple. Supports tuples up to size 9."
  [v]
  (cond
    (instance? MapEntry v)
    (Tuple2. (key v) (val v))

    (< (count v) 10)
    (apply tuple v)

    :else
    (throw (IllegalArgumentException.
             (str "Cannot coerce value to a tuple: " (pr-str v))))))


(defn tuple->vec
  "Coerce a Scala tuple to a Clojure vector. Supports tuples up to size 9."
  [v]
  (condp instance? v
    Tuple1
    (let [t ^Tuple1 v]
      (vector (._1 t)))

    Tuple2
    (let [t ^Tuple2 v]
      (vector (._1 t) (._2 t)))

    Tuple3
    (let [t ^Tuple3 v]
      (vector (._1 t) (._2 t) (._3 t)))

    Tuple4
    (let [t ^Tuple4 v]
      (vector (._1 t) (._2 t) (._3 t) (._4 t)))

    Tuple5
    (let [t ^Tuple5 v]
      (vector (._1 t) (._2 t) (._3 t) (._4 t) (._5 t)))

    Tuple6
    (let [t ^Tuple6 v]
      (vector (._1 t) (._2 t) (._3 t) (._4 t) (._5 t) (._6 t)))

    Tuple7
    (let [t ^Tuple7 v]
      (vector (._1 t) (._2 t) (._3 t) (._4 t) (._5 t) (._6 t) (._7 t)))

    Tuple8
    (let [t ^Tuple8 v]
      (vector (._1 t) (._2 t) (._3 t) (._4 t) (._5 t) (._6 t) (._7 t) (._8 t)))

    Tuple9
    (let [t ^Tuple9 v]
      (vector (._1 t) (._2 t) (._3 t) (._4 t) (._5 t) (._6 t) (._7 t) (._8 t) (._9 t)))

    (throw (IllegalArgumentException.
             (str "Cannot coerce " (class v) " value to a vector")))))


(defn from-tuple
  "Coerce a Scala tuple value to a Clojure vector. Recursively walks the
  structure to ensure all nested tuples are converted."
  [t]
  (letfn [(coerce-product
            [x]
            (if (instance? Product x)
              (tuple->vec x)
              x))]
    (walk/prewalk coerce-product t)))


(defn from-pair
  "Coerce a Scala pair (`Tuple2`) value to a Clojure value. Returns map entry
  values for efficiency. Recursively walks the structure to ensure all nested
  values are Clojure-compatible."
  [^Tuple2 pair]
  (MapEntry. (from-tuple (._1 pair)) (from-tuple (._2 pair))))


(defn to-pair
  "Coerce a Clojure value to a Scala pair (`Tuple2`)."
  ^Tuple2
  [entry]
  (cond
    ;; Null values can't be coerced.
    (nil? entry)
    (throw (IllegalArgumentException.
             "Cannot coerce nil to a pair value"))

    ;; Scala tuples can be returned directly.
    (instance? Tuple2 entry)
    entry

    ;; Use key/value from map entries to construct the pair.
    (instance? MapEntry entry)
    (Tuple2. (key entry) (val entry))

    ;; Try to generically coerce a vector result.
    (vector? entry)
    (if (= 2 (count entry))
      (Tuple2. (clojure.core/first entry) (clojure.core/second entry))
      (throw (IllegalArgumentException.
               (str "Cannot coerce a vector with " (count entry)
                    " elements to a pair value"))))

    ;; Unknown type, can't coerce.
    :else
    (throw (IllegalArgumentException.
             (str "Cannot coerce unknown type " (.getName (class entry))
                  " to a pair value")))))


(defn first
  "Get the first element of a Scala pair."
  [^Tuple2 t]
  (._1 t))


(defn second
  "Get the second element of a Scala pair."
  [^Tuple2 t]
  (._2 t))
