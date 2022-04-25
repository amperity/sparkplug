(ns sparkplug.function-test
  (:require
    [clojure.test :refer [are deftest is]]
    [sparkplug.function :as f]))


(def this-ns (ns-name *ns*))


(defprotocol TestProto

  (proto-method [this])

  (get-closure [this]))


(defrecord TestRecord
  [example-fn]

  TestProto

  (proto-method
    [this]
    (example-fn))


  (get-closure
    [this]
    (fn inside-fn
      []
      nil)))


(deftest resolve-namespace-references
  (are [expected-references obj] (= expected-references (f/namespace-references obj))

    ;; Simple data
    #{} nil
    #{} :keyword
    #{} 5
    #{} true
    #{} "str"
    #{} 'sym

    ;; Functions
    #{this-ns}
    (fn [])

    #{this-ns 'sparkplug.function}
    (fn []
      (f/namespace-references (fn [])))

    #{this-ns 'sparkplug.function}
    (fn []
      (let [x (f/namespace-references (fn []))]
        (x)))

    #{this-ns}
    [(fn [])]

    #{this-ns}
    (list (fn []))

    #{this-ns}
    (doto (java.util.ArrayList.)
      (.add (fn [])))

    #{this-ns}
    (doto (java.util.HashMap.)
      (.put "key" (fn [])))

    #{this-ns}
    {:key (fn [])}

    #{this-ns}
    {:key {:nested (fn [])}}

    #{this-ns}
    {:key {:nested [(fn [])]}}

    ;; Record fields.
    #{this-ns 'sparkplug.function}
    (->TestRecord
      (fn []
        (f/namespace-references nil)))

    ;; Function that closes over an object invoking a protocol method.
    #{this-ns 'sparkplug.function}
    (let [inst (->TestRecord
                 (fn []
                   (f/namespace-references nil)))]
      (fn [] (proto-method inst)))

    ;; Function closure defined inside a record class.
    #{this-ns}
    (let [x (->TestRecord nil)]
      (get-closure x))))
