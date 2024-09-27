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
    [sparkplug.function.test-fns :as test-fns]
    [sparkplug.kryo :as kryo]
    [sparkplug.rdd :as rdd]
    [sparkplug.scala :as scala])
  (:import
    com.esotericsoftware.kryo.Kryo
    (java.io
      ByteArrayInputStream
      ByteArrayOutputStream
      ObjectInputStream
      ObjectOutputStream)))


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
            (if (<= 32 (int c))
              c
              \.)))
    (partition-all 32)
    (map str/join)
    (str/join "\n")
    (println)))


(defn serialize
  [f]
  (let [baos (ByteArrayOutputStream.)]
    (with-open [out (ObjectOutputStream. baos)]
      (.writeObject out f))
    (.toByteArray baos)))


(defn deserialize
  [bs]
  (with-open [in (ObjectInputStream. (ByteArrayInputStream. bs))]
    (.readObject in)))
