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
    [sparkplug.kryo :as kryo]
    [sparkplug.rdd :as rdd]
    [sparkplug.scala :as scala])
  (:import
    com.esotericsoftware.kryo.Kryo))


(def local-conf
  (-> (conf/spark-conf)
      (conf/master "local[*]")
      (conf/app-name "user")))


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
     (->
       (->>
         (rdd/text-file ctx (str "file://" path))
         (spark/map str/lower-case)
         (spark/mapcat seq)
         (spark/map->pairs #(vector % 1))
         (spark/reduce-by-key +)
         (spark/into {}))
       (as-> result
         (do (println "Done, press enter to continue...")
             (read-line)
             result))))))


(def kryo
  (delay (kryo/initialize)))


(defn inspect-bytes
  [data]
  (->>
    (seq data)
    (map #(let [c (char (if (neg? %)
                          (+ % 256)
                          %))]
            (if (or (Character/isLetterOrDigit c)
                    (Character/isWhitespace c))
              c
              \.)))
    (str/join)
    (println)))
