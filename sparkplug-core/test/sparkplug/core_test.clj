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
      (conf/app-name "user")
      (conf/set-param "spark.ui.enabled" "false")))


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
                (spark/into [])))))

  (testing "sort-by"
    (is (= (vec (reverse (range 10)))
           (->> (rdd/parallelize *sc* (shuffle (range 10)))
                (spark/sort-by -)
                (spark/into []))
           (->> (rdd/parallelize *sc* (shuffle (range 10)))
                (spark/sort-by identity false)
                (spark/into [])))))

  (testing "union"
    (is (= #{:a :b}
           (spark/into #{} (spark/union (rdd/parallelize *sc* [:a :b])))))
    (is (= #{:a :b :c :d}
           (spark/into
             #{}
             (spark/union
               (rdd/parallelize *sc* [:a :b])
               (rdd/parallelize *sc* [:c :d])))))
    (is (= #{:a :b :c :d :e :f}
           (spark/into
             #{}
             (spark/union
               (rdd/parallelize *sc* [:a :b])
               (rdd/parallelize *sc* [:c :d])
               (rdd/parallelize *sc* [:e :f])))))
    (is (= #{[:a :b]}
           (spark/into #{} (spark/union (rdd/parallelize-pairs *sc* [[:a :b]])))))
    (is (= #{[:a :b] [:c :d]}
           (spark/into
             #{}
             (spark/union
               (rdd/parallelize-pairs *sc* [[:a :b]])
               (rdd/parallelize-pairs *sc* [[:c :d]])))))
    (is (= #{[:a :b] [:c :d] [:e :f]}
           (spark/into
             #{}
             (spark/union
               (rdd/parallelize-pairs *sc* [[:a :b]])
               (rdd/parallelize-pairs *sc* [[:c :d]])
               (rdd/parallelize-pairs *sc* [[:e :f]])))))))
