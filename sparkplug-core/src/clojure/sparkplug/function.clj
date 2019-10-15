(ns sparkplug.function
  "This namespace generates function classes for interop with Spark."
  (:require
    [clojure.tools.logging :as log]
    [clojure.string :as str]))


(def ^:private unbound-fn-re
  #"Attempting to call unbound fn: #'([^/]+\/.*)")


(defn- unbound-fn
  [^Exception ex]
  (some->>
    (ex-message ex)
    (re-find unbound-fn-re)
    (second)
    (symbol)))


(defn- with-unbound-retry
  ([f] (with-unbound-retry f #{}))
  ([f required]
   (fn [& args]
     (try
       (apply f args)
       (catch Exception ex
         (if-let [unbound (unbound-fn ex)]
           (if-not (contains? required unbound)
             (let [required' (conj required unbound)]
               (log/warn "Caught unbound function" unbound "attempting to require and rerun.")
               (requiring-resolve unbound)
               (apply (with-unbound-retry f required') args))
             (do
               (log/error "Previously required" unbound "not attempting to re-require.")
               (throw ex)))
           (throw ex)))))))


(defmacro ^:private gen-function
  "Generate a new constructor for functions of the `fn-name` class that extends
  `SerializableFn` and implements interfaces for compatibility with Spark."
  [fn-name constructor]
  (let [class-sym (symbol (str "sparkplug.function." fn-name))]
    ^:cljfmt/ignore
    `(defn ~(vary-meta constructor assoc :tag class-sym)
       ~(str "Construct a new serializable " fn-name " function wrapping `f`.")
       [~'f]
       (new ~class-sym (with-unbound-retry ~'f)))))


(gen-function Fn1 fn1)
(gen-function Fn2 fn2)
(gen-function Fn3 fn3)
(gen-function ComparatorFn comparator-fn)
(gen-function FlatMapFn1 flat-map-fn)
(gen-function FlatMapFn2 flat-map-fn2)
(gen-function PairFlatMapFn pair-flat-map-fn)
(gen-function PairFn pair-fn)
(gen-function VoidFn void-fn)
