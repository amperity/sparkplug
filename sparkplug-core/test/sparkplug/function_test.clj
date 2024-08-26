(ns sparkplug.function-test
  (:require
    [clojure.test :refer [are deftest is testing]]
    [sparkplug.function :as f]
    [sparkplug.function.test-fns :as test-fns])
  (:import
    (java.io
      ByteArrayInputStream
      ByteArrayOutputStream
      ObjectInputStream
      ObjectOutputStream)))


(def this-ns
  (ns-name *ns*))


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


;; This is a regression test which ensures that decoded functions which close
;; over a boolean value are updated to use the canonical `Boolean` static
;; instances. Otherwise, users see bugs where a false value evaluates as truthy.
(deftest canonical-booleans
  (letfn [(serialize
            [f]
            (let [baos (ByteArrayOutputStream.)]
              (with-open [out (ObjectOutputStream. baos)]
                (.writeObject out f))
              (.toByteArray baos)))

          (deserialize
            [bs]
            (with-open [in (ObjectInputStream. (ByteArrayInputStream. bs))]
              (.readObject in)))]
    (testing "closure over true value"
      (let [original-fn (f/fn1 (test-fns/bool-closure true))
            decoded-fn (-> original-fn serialize deserialize)]
        (testing "original behavior"
          (is (= :x (.call original-fn :x))
              "should return value"))
        (testing "decoded behavior"
          (is (= :x (.call decoded-fn :x))
              "should return value"))))
    (testing "closure over false value"
      (let [original-fn (f/fn1 (test-fns/bool-closure false))
            decoded-fn (-> original-fn serialize deserialize)]
        (testing "original behavior"
          (is (nil? (.call original-fn :x))
              "should not return value"))
        (testing "decoded behavior"
          (is (nil? (.call decoded-fn :x))
              "should not return value"))))))
