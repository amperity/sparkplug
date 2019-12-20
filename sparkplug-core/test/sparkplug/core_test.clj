(ns sparkplug.core-test
  (:require
    [clojure.test :refer [deftest is testing use-fixtures]]
    [sparkplug.config :as conf]
    [sparkplug.context :as context]
    [sparkplug.core :as spark]
    [sparkplug.rdd :as rdd]))


(def ^:dynamic *sc*
  nil)


(def local-conf
  (-> (conf/spark-conf)
      (conf/master "local[*]")
      (conf/app-name "user")))


(defn spark-context-fixture
  [f]
  (context/with-context [sc local-conf]
    (binding [*sc* sc]
      (f))))


(use-fixtures :once spark-context-fixture)


(deftest core-transforms
  (testing "aggregate-by-key"
    (is (= [[1 (reduce + (range 10))]]
           (->> (rdd/parallelize-pairs *sc* (map vector (repeat 10 1) (range 10)))
                (spark/aggregate-by-key + + 0)
                (spark/into []))))
    (is (= [[1 (reduce + (range 10))]]
           (->> (rdd/parallelize-pairs *sc* (map vector (repeat 10 1) (range 10)))
                (spark/aggregate-by-key + + 0 2)
                (spark/into []))))
    (is (= [[1 (reduce + (range 10))]]
           (->> (rdd/parallelize-pairs *sc* (map vector (repeat 10 1) (range 10)))
                (spark/aggregate-by-key + + 0 (rdd/hash-partitioner 2))
                (spark/into []))))))
