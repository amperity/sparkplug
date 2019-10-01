(ns sparkplug.kryo-test
  (:require
    [clojure.test :refer [are deftest testing is]]
    [clojure.test.check.clojure-test :refer [defspec]]
    [clojure.test.check.generators :as gen]
    [clojure.test.check.properties :as prop]
    [sparkplug.kryo :as kryo])
  (:import
    com.esotericsoftware.kryo.Kryo
    (com.esotericsoftware.kryo.io
      Input
      Output)))


(deftest classpath-search
  (let [registries (kryo/classpath-registries)]
    (is (sequential? registries))
    (is (<= 2 (count registries)))))


(defn- serialize
  ^bytes [^Kryo kryo obj]
  (let [output (Output. 4096 -1)]
    (.writeClassAndObject kryo output obj)
    (.flush output)
    (.getBuffer output)))


(defn- deserialize
  [^Kryo kryo ^bytes data]
  (let [input (Input. data)]
    (.readClassAndObject kryo input)))


(defn- replace-nans
  [x]
  (clojure.walk/postwalk
    #(if (and (number? %) (Double/isNaN %))
       :NaN
       %)
    x))


(def kryo (kryo/initialize))


(defspec clojure-kryo 1000
  (prop/for-all [x gen/any]
    (is (= (replace-nans x)
           (->> x
                (serialize kryo)
                (deserialize kryo)
                replace-nans)))))
