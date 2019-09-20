(ns sparkplug.function
  "This namespace generates function classes for interop with Spark.")


(defmacro ^:private gen-function
  "Generate a new constructor for functions of the `fn-name` class that extends
  `SerializableFn` and implements interfaces for compatibility with Spark."
  [fn-name constructor]
  (let [class-sym (symbol (str "sparkplug.function." fn-name))]
    ^:cljfmt/ignore
    `(defn ~(vary-meta constructor assoc :tag class-sym)
       ~(str "Construct a new serializable " fn-name " function wrapping `f`.")
       [~'f]
       (new ~class-sym ~'f))))


(gen-function Fn1 fn1)
(gen-function Fn2 fn2)
(gen-function Fn3 fn3)
(gen-function ComparatorFn comparator-fn)
(gen-function FlatMapFn1 flat-map-fn)
(gen-function FlatMapFn2 flat-map-fn2)
(gen-function PairFlatMapFn pair-flat-map-fn)
(gen-function PairFn pair-fn)
(gen-function VoidFn void-fn)
