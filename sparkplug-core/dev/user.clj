(ns user
  (:require
    [clojure.java.io :as io]
    [clojure.repl :refer :all]
    [clojure.stacktrace :refer [print-cause-trace]]
    [clojure.string :as str]
    [clojure.tools.namespace.repl :refer [refresh]]
    [sparkplug.bool-repro :as bool]
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
                    (and (Character/isWhitespace c)
                         (not= \newline c)))
              c
              \.)))
    (partition-all 32)
    (map str/join)
    (str/join "\n")
    (println)))


;; Boolean Shenanigans Reproduction

(defn access-field
  [value field]
  (#'f/access-field field value))


(def serializable-fn-field
  (nth (.getDeclaredFields sparkplug.function.SerializableFn) 2))


(def test-fn-inner-class
  (class (access-field (bool/make-test-fn true) serializable-fn-field)))


(def test-fn-boolean-field
  (first (.getDeclaredFields test-fn-inner-class)))


(defn get-inner-boolean
  [sf]
  (-> sf
      (access-field serializable-fn-field)
      (access-field test-fn-boolean-field)))


(defn encode-fn
  [sf]
  (let [baos (java.io.ByteArrayOutputStream.)]
    (with-open [out (java.io.ObjectOutputStream. baos)]
      (.writeObject out sf))
    (.toByteArray baos)))


(defn decode-fn
  [bs]
  (with-open [in (java.io.ObjectInputStream. (java.io.ByteArrayInputStream. bs))]
    (.readObject in)))
