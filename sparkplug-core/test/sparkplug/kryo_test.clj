(ns sparkplug.kryo-test
  (:require
    [clojure.test :refer [are deftest testing is]]
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


(deftest clojure-serialization
  (are [x] (let [kryo (kryo/initialize)]
             (= x (->> x (serialize kryo) (deserialize kryo))))

    5
    5/7
    'foo
    "foo"
    :foo
    [1]
    #{"a"}
    {"a" "b"}
    {:foo "bar"}))
