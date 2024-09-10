(ns sparkplug.function.test-fns
  "AOT-compiled test functions.")


(defn bool-closure
  [b]
  (fn inner
    [x]
    (when b
      x)))
