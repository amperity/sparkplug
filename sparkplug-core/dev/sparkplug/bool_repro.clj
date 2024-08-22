(ns sparkplug.bool-repro
  (:require
    [sparkplug.function :as f]))


(defn make-test-fn
  [b]
  (f/fn1 (fn inner [x] (when b x))))
