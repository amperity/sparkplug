(ns hooks.sparkplug.function
  (:require
    [clj-kondo.hooks-api :as api]))


(defn gen-function
  "Macro analysis for `sparkplug.function/gen-function`."
  [form]
  (let [name-sym (-> form :node :children (nth 2))
        constructor (api/list-node
                      [(api/token-node 'defn)
                       name-sym
                       (api/vector-node
                         [(api/token-node '_f)])])]
    {:node constructor}))
