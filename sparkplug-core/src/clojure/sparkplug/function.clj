(ns sparkplug.function
  "This namespace generates function classes for various kinds of interop with
  Spark and Scala. This namespace **must** be AOT compiled before using Spark."
  (:refer-clojure :exclude [fn])
  (:require
    [clojure.set :as set])
  (:import
    sparkplug.function.SerializableFn))


;; ## Function Wrappers

(defmacro ^:private gen-function
  "Generate a new function class that extends `SerializableFn` and implements
  interfaces for compatibility with Spark and Scala.

  Outputs a new class named by `clazz` with a constructor function named by
  `constructor`. The class will implement the interface of the same name in
  `org.apache.spark.api.java.function`."
  [clazz constructor]
  ^:cljfmt/ignore
  `(defn ~constructor
     ~(str "Construct a new serializable " clazz " function wrapping `f`.")
     [~'f]
     (if-let [namespaces# (::requires (meta ~'f))]
       (new ~(symbol  (str "sparkplug.function." clazz)) ~'f namespaces#)
       (new ~(symbol  (str "sparkplug.function." clazz)) ~'f))))


(gen-function Fn1 fn1)
(gen-function Fn2 fn2)
(gen-function Fn3 fn3)
(gen-function ComparatorFn comparator-fn)
(gen-function FlatMapFn1 flat-map-fn)
(gen-function FlatMapFn2 flat-map-fn2)
(gen-function PairFlatMapFn pair-flat-map-fn)
(gen-function PairFn pair-fn)
(gen-function VoidFn void-fn)
