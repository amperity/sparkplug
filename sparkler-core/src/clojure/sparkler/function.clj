(ns sparkler.function
  "This namespace generates function classes for various kinds of interop with
  Spark and Scala. This namespace **must** be AOT compiled before using Spark."
  (:refer-clojure :exclude [fn])
  (:require
    [clojure.set :as set])
  (:import
    sparkler.function.SerializableFn))


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
       (new ~(symbol  (str "sparkler.function." clazz)) ~'f namespaces#)
       (new ~(symbol  (str "sparkler.function." clazz)) ~'f))))


(gen-function Fn1 fn1)
(gen-function Fn2 fn2)
(gen-function Fn3 fn3)
(gen-function VoidFn void-fn)
(gen-function ComparatorFn comparator-fn)
(gen-function FlatMapFn1 flat-map-fn)
(gen-function FlatMapFn2 flat-map-fn2)
(gen-function PairFlatMapFn pair-flat-map-fn)
(gen-function PairFn pair-fn)



;; ## Inline Functions

;; TODO: decide if these are useful

#_
(defn- anonymous-function-name
  "Generate a deterministic name for an anonymous function by referencing the
  ns and source code."
  [form-ns line column]
  (->
    (str form-ns "_anon_" line \_ column)
    (clojure.lang.Compiler/munge)
    (symbol)))


#_
(defn- ns-requires
  "Discover all namespaces required to run code from `form-ns` by examining
  the `ns-refers` and `ns-aliases`. Returns a set of required namespaces as
  symbols.

  **NOTE:** This won't catch fully-qualified references to other namespaces!"
  [form-ns]
  ;; TODO: are there more namespaces that can be ignored here?
  (let [implicit? #{"clojure.core"
                    "clojure.java.io"}]
    (set/union
      ;; Namespaces referenced by a local alias.
      (into #{}
            (comp
              (map val)
              (map str)
              (remove implicit?)
              (map symbol))
            (ns-aliases form-ns))
      ;; Namespaces referenced by a referred function.
      (into #{}
            (comp
              (map val)
              (map symbol)
              (map namespace)
              (remove implicit?)
              (map symbol))
            (ns-refers form-ns)))))


#_
(defmacro fn
  "Define a function as in `clojure.core/fn` but ensures the result is
  serializable for use with Spark. Only accepts a single arity signature.

      (fn name? [params*] exprs*)

  If a name is not provided, one will be deterministically generated."
  [& signature]
  (let [form-meta (meta &form)
        line (:line form-meta)
        column (:column form-meta)
        [fn-name args & body] (if (symbol? (first signature))
                                signature
                                (cons (anonymous-function-name *ns* line column)
                                      signature))
        namespaces (ns-requires *ns*)]
    `(vary-meta
       (clojure.core/fn ~fn-name ~args ~@body)
       assoc :requires namespaces)))
