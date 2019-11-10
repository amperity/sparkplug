(ns sparkplug.function
  "This namespace generates function classes for various kinds of interop with
  Spark and Scala."
  (:require
    [clojure.string :as str])
  (:import
    (java.lang.reflect
      Field
      Modifier)
    java.util.HashSet
    sparkplug.function.SerializableFn))


;; ## Namespace Discovery

(defn- access-field
  "Attempt to get a field on the given object by making sure it is accessible.
  Returns the field value on success, or nil on failure."
  [^Field field obj]
  (let [accessible? (.isAccessible field)]
    (try
      (when-not accessible?
        (.setAccessible field true))
      (.get field obj)
      (catch IllegalAccessException ex
        ;; TODO: warn?
        nil))))


(defn- walk-object-vars
  "Walk the given object to find namespaces referenced by vars. Adds discovered
  reference symbols to `references` and tracks values in `visited`."
  [^HashSet references ^HashSet visited obj]
  (when-not (or (nil? obj)
                ;; Simple types that can't have namespace references.
                (boolean? obj)
                (string? obj)
                (number? obj)
                (keyword? obj)
                (symbol? obj)
                (instance? clojure.lang.Ref obj)
                ;; Nothing to do if we've already visited this object.
                (.contains visited obj))
    (.add visited obj)
    (if (var? obj)
      ;; Vars directly represent a namespace dependency.
      (let [ns-sym (ns-name (:ns (meta obj)))]
        (.add references ns-sym))
      ;; Otherwise, traverse the object.
      (do
        ;; For maps and records, traverse over their contents in addition to
        ;; their fields.
        (when (map? obj)
          (doseq [entry obj]
            (walk-object-vars references visited entry)))
        ;; Traverse the fields of the value for more references.
        (doseq [^Field field (.getDeclaredFields (class obj))]
          ;; Only traverse static fields for maps.
          (when (or (not (map? obj)) (Modifier/isStatic (.getModifiers field)))
            (let [value (access-field field obj)]
              (when (or (ifn? value) (map? value))
                (walk-object-vars references visited value)))))))))


(defn namespace-references
  "Walk the given function-like object to find all namespaces referenced by
  closed-over vars. Returns a set of referenced namespace symbols."
  [^Object obj]
  (let [;; Attempt to derive the needed Clojure namespace
        ;; from the function's class name.
        obj-ns (-> (.. obj getClass getName)
                   (Compiler/demunge)
                   (str/split #"/")
                   (first)
                   (symbol))
        references (doto (HashSet.) (.add obj-ns))
        visited (HashSet.)]
    (walk-object-vars references visited obj)
    (disj (set references) 'clojure.core)))


;; ## Function Wrappers

(defmacro ^:private gen-function
  "Generate a new constructor for functions of the `fn-name` class that extends
  `SerializableFn` and implements interfaces for compatibility with Spark."
  [fn-name constructor]
  (let [class-sym (symbol (str "sparkplug.function." fn-name))]
    ^:cljfmt/ignore
    `(defn ~(vary-meta constructor assoc :tag class-sym)
       ~(str "Construct a new serializable " fn-name " function wrapping `f`.")
       [~'f]
       (let [references# (namespace-references ~'f)]
         (new ~class-sym ~'f (mapv str references#))))))


(gen-function Fn1 fn1)
(gen-function Fn2 fn2)
(gen-function Fn3 fn3)
(gen-function ComparatorFn comparator-fn)
(gen-function FlatMapFn1 flat-map-fn)
(gen-function FlatMapFn2 flat-map-fn2)
(gen-function PairFlatMapFn pair-flat-map-fn)
(gen-function PairFn pair-fn)
(gen-function VoidFn void-fn)
