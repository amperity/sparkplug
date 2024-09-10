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

(defn- fn-enclosing-class
  "Given a function object, determine the name of the class which the function
  is a child of. Usually this is the class representing the namespace where the
  function is defined."
  [f]
  (-> (.getName (class f))
      (Compiler/demunge)
      (str/split #"/")
      (first)
      (symbol)))


(defn- class-name?
  "True if the provided symbol names a class, rather than a namespace."
  [name-sym]
  (let [class-name (-> (str name-sym)
                       (str/replace "-" "_")
                       (symbol))]
    (class? (resolve class-name))))


(defn- type-namespace
  "Given a symbol naming a record, return a symbol naming its defining
  namespace if it exists."
  [name-sym]
  (let [ns-sym (-> (str name-sym)
                   (str/replace #"\.[^.]+$" "")
                   (symbol))]
    (when (find-ns ns-sym)
      ns-sym)))


(defn- fn-namespace
  "Given a function object, derive the name of the namespace where it was
  defined."
  [f]
  ;; The logic here is to avoid marking class names as namespaces to be
  ;; required. When using a piece of data as a function, such as a keyword or
  ;; set, this will actually be a class name like `clojure.lang.Keyword`. This
  ;; also happens when referencing a function closure defined inside of a
  ;; record implementation, since the function becomes an inner class; in that
  ;; case, we _do_ want to mark the record's defining namespace.
  (let [enclosing (fn-enclosing-class f)]
    (if (class-name? enclosing)
      (type-namespace enclosing)
      enclosing)))


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
      (when-let [ns-sym (fn-namespace obj)]
        (.add references ns-sym)
        (doseq [^Field field (.getDeclaredFields (class obj))]
          (let [value (SerializableFn/accessField obj field)]
            (walk-object-refs references visited value))))

      ;; For collection-like objects, (e.g. vectors, maps, records, Java collections),
      ;; just traverse the objects they contain.
      (seqable? obj)
      (doseq [entry obj]
        (walk-object-refs references visited entry))

      ;; Otherwise, reflectively traverse the fields of the object for more references.
      :else
      (doseq [^Field field (.getDeclaredFields (class obj))]
        (when-not (Modifier/isStatic (.getModifiers field))
          (let [value (SerializableFn/accessField obj field)]
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
