(ns user
  (:require
    [clojure.java.io :as io]
    [clojure.repl :refer :all]
    [clojure.stacktrace :refer [print-cause-trace]]
    [clojure.string :as str]
    [clojure.tools.namespace.repl :refer [refresh]]
    [sparkplug.core :as spark]
    [sparkplug.function :as f]
    [sparkplug.rdd :as rdd]))
