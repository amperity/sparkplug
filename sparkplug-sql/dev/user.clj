(ns user
  (:require
    [clj-antlr.core :as antlr]
    [clojure.java.io :as io]))


(alter-var-root
  #'clj-antlr.common/case-changing-char-stream
  (fn [f]
    (fn [charstream lower?]
      (f charstream (not lower?)))))


(defn parser
  [grammar]
  (antlr/parser grammar {:case-sensitive? false}))


(def p
  (parser (str (io/file (io/resource "SqlBase.g4")))))



(tree-seq
  (complement string?)
  rest
  (p "select a.id, b.foo from tablea a join tableb b"))


;; what columns are being output
;; what table/columns are they sourced from
