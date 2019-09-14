(ns sparkler.name
  "Utilities for working with function, class, and RDD names."
  (:require
    [clojure.string :as str])
  (:import
    org.apache.spark.rdd.RDD))


(defn- internal-call?
  "True if a stack-trace element should be ignored because it represents an internal
  function call that should not be considered a callsite."
  [^StackTraceElement element]
  (let [class-name (.getClassName element)]
    (or (str/starts-with? class-name "sparkler.")
        (str/starts-with? class-name "clojure.lang."))))


(defn- stack-callsite
  "Find the top element in the current stack trace that is not an internal
  function call."
  ^StackTraceElement
  []
  (first (remove internal-call? (.getStackTrace (Exception.)))))


(defn- unmangle
  "Given the name of a class that implements a Clojure function, returns the
  function's name in Clojure.

  If the true Clojure function name contains any underscores (a rare
  occurrence), the unmangled name will contain hyphens at those locations
  instead."
  [classname]
  (-> classname
      (str/replace #"^(.+)\$(.+)(|__\d+)$" "$1/$2")
      (str/replace \_ \-)))


(defn fn-name
  "Return the (unmangled) name of the given Clojure function."
  [f]
  (unmangle (.getName (class f))))


(defn set-callsite-name
  "Provide a name for the given RDD by looking at the current stack. Returns
  the updated RDD if the name could be determined."
  ^org.apache.spark.rdd.RDD
  [^RDD rdd & args]
  (try
    (let [callsite (stack-callsite)
          filename (.getFileName callsite)
          classname (.getClassName callsite)
          line-number (.getLineNumber callsite)
          rdd-name (format "#<%s: %s %s:%d%s>"
                           (.getSimpleName (class rdd))
                           (unmangle classname)
                           filename
                           line-number
                           (if (seq args)
                             (str " [" (str/join ", " args) "]")
                             ""))]
      (.setName rdd rdd-name))
    (catch Exception e
      ;; Ignore errors and return an unnamed RDD.
      rdd)))
