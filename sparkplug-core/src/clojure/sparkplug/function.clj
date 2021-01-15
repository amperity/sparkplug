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


(defn- fn-namespace
  "Given a function object, derive the name of the namespace where it was
  defined."
  [obj]
  (-> (.getName (class obj))
      (Compiler/demunge)
      (str/split #"/")
      (first)
      (symbol)))


(defn- walk-object-refs
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
    (cond
      ;; Vars directly represent a namespace dependency.
      (var? obj)
      (let [ns-sym (ns-name (:ns (meta obj)))]
        (.add references ns-sym))

      ;; Clojure functions:
      ;; Try to derive the namespace that defined the function.
      ;; Functions also have Var references as static fields,
      ;; and have closed-over objects as non-static fields.
      (fn? obj)
      (let [ns-sym (fn-namespace obj)]
        ;; When using a piece of data as a function, such as a keyword or set,
        ;; this will actually be a class name like `clojure.lang.Keyword`.
        ;; Avoid marking class names as namespaces to be required.
        (when-not (class? (resolve ns-sym))
          (.add references ns-sym)
          (doseq [^Field field (.getDeclaredFields (class obj))]
            (let [value (access-field field obj)]
              (walk-object-refs references visited value)))))

      ;; For collection-like objects, (e.g. vectors, maps, records, Java collections),
      ;; just traverse the objects they contain.
      (seqable? obj)
      (do
        (doseq [entry obj]
          (walk-object-refs references visited entry)))

      ;; Otherwise, reflectively traverse the fields of the object for more references.
      :else
      (doseq [^Field field (.getDeclaredFields (class obj))]
        (when-not (Modifier/isStatic (.getModifiers field))
          (let [value (access-field field obj)]
            (walk-object-refs references visited value)))))))


(defn namespace-references
  "Walk the given function-like object to find all namespaces referenced by
  closed-over vars. Returns a set of referenced namespace symbols."
  [^Object obj]
  (let [references (HashSet.)
        visited (HashSet.)]
    (walk-object-refs references visited obj)
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
