(ns user
  (:require
    [clojure.java.io :as io]
    [clojure.repl :refer :all]
    [clojure.stacktrace :refer [print-cause-trace]]
    [clojure.string :as str]
    [clojure.tools.namespace.repl :refer [refresh]]
    [sparkplug.config :as conf]
    [sparkplug.context :as ctx]
    [sparkplug.core :as spark]
    [sparkplug.function :as f]
    [sparkplug.rdd :as rdd]
    [sparkplug.scala :as scala]))


(def spark-context nil)


(defn letter-frequencies
  "Calculate the number of times each letter appears in the given text."
  ([]
   (letter-frequencies "/usr/share/dict/words"))
  ([path]
   (ctx/with-context [ctx (-> (conf/spark-conf)
                              (conf/master "local[2]")
                              (conf/app-name "letter-frequencies"))]
     (alter-var-root #'spark-context (constantly ctx))
     (->>
       (rdd/text-file ctx (str "file://" path))
       (spark/map str/lower-case)
       (spark/mapcat seq)
       (spark/map->pairs #(vector % 1))
       (spark/reduce-by-key +)
       (spark/into {})))))
