(ns sparkplug.repl.work
  (:require
    [clojure.java.io :as io]
    [clojure.repl :refer :all]
    [clojure.set :as set]
    [clojure.stacktrace :refer [print-cause-trace]]
    [clojure.string :as str]
    [sparkplug.config :as conf]
    [sparkplug.context :as ctx]
    [sparkplug.core :as spark]
    [sparkplug.function :as f]
    [sparkplug.kryo :as kryo]
    [sparkplug.rdd :as rdd]
    [sparkplug.scala :as scala]))


(def spark-context
  "The currently active Spark context."
  nil)


(defn exit!
  "Exit the running REPL gracefully."
  []
  (deliver @(resolve 'sparkplug.repl.main/exit-promise) :exit))
