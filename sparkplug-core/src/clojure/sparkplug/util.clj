(ns sparkplug.util
  "Commonly used utilities for working with functions, classes, and Scala
  objects."
  (:require
    [clojure.string :as str]))


;; ## Naming

(defn- internal-call?
  "True if a stack-trace element should be ignored because it represents an internal
  function call that should not be considered a callsite."
  [^StackTraceElement element]
  (let [class-name (.getClassName element)]
    (or (str/starts-with? class-name "sparkplug.")
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


(defn callsite-name
  "Generate a name for the callsite of this function by looking at the current
  stack. Ignores core Clojure and internal function frames."
  []
  (let [callsite (stack-callsite)
        filename (.getFileName callsite)
        classname (.getClassName callsite)
        line-number (.getLineNumber callsite)]
    (format "%s %s:%d" (unmangle classname) filename line-number)))
