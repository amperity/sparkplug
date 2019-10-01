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


(defn- replace-incomparable-types
  "Replace regex patterns and NaN with keywords because they are not
  comparable by value."
  [x]
  (walk/postwalk
    #(cond (and (number? %) (Double/isNaN %)) :NaN
           (instance? java.util.regex.Pattern %) :regex
           :else %)
    x))


(def kryo (kryo/initialize))


(defspec clojure-data-roundtrip 1000
  (prop/for-all [x (gen/fmap replace-incomparable-types gen/any)]
    (is (= x (->> x (kryo/encode kryo) (kryo/decode kryo))))))
