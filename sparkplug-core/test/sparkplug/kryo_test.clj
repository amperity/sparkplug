(ns sparkplug.kryo-test
  (:require
    [clojure.test :refer [are deftest testing is]]
    [clojure.test.check.clojure-test :refer [defspec]]
    [clojure.test.check.generators :as gen]
    [clojure.test.check.properties :as prop]
    [clojure.walk :as walk]
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


(def kryo (kryo/initialize))


(defspec clojure-data-roundtrip
  {:num-tests 1000
   :max-size 20}
  (prop/for-all [x gen/any-equatable]
    (println (pr-str x))
    (is (= x (->> x (kryo/encode kryo) (kryo/decode kryo))))))
