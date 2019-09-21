(ns sparkplug.kryo-test
  (:require
    [clojure.test :refer [deftest testing is]]
    [sparkplug.kryo :as kryo]))


(deftest classpath-search
  (let [registries (kryo/classpath-registries)]
    (is (sequential? registries))
    (is (<= 2 (count registries)))))
