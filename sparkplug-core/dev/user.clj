(ns user
  (:require
    [clojure.java.io :as io]
    [clojure.repl :refer :all]
    [clojure.stacktrace :refer [print-cause-trace]]
    [clojure.string :as str]
    [clojure.tools.namespace.repl :refer [refresh]])
  (:import
    sparkplug.function.SerializableFn))


(defn find-var-namespaces
  "Trace the given value to find referenced namespaces."
  [f]
  (set (SerializableFn/findVarNamespaces f)))
