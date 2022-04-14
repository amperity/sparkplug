(defproject amperity/sparkplug-repl "0.1.9-SNAPSHOT"
  :description "Clojure REPL for Spark exploration"
  :url "https://github.com/amperity/sparkplug"
  :scm {:dir ".."}
  :license {:name "Apache License 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0"}

  :monolith/inherit true

  :dependencies
  [[org.clojure/clojure "1.10.1"]
   [amperity/sparkplug-core "0.1.9-SNAPSHOT"]
   [mvxcvi/whidbey "2.1.1"]
   [nrepl "0.6.0"]]

  :main sparkplug.repl.main

  :profiles
  {:default
   [:base :system :user :provided :spark-3.1 :dev]

   :repl
   {:repl-options
    {:custom-init (whidbey.repl/update-print-fn!)
     :init-ns sparkplug.repl.work}}

   :spark-3.1
   ^{:pom-scope :provided}
   {:dependencies
    [[org.apache.spark/spark-core_2.12 "3.1.3"]
     [org.apache.spark/spark-sql_2.12 "3.1.3"]]}

   :spark-3.2
   ^{:pom-scope :provided}
   {:dependencies
    [[org.apache.spark/spark-core_2.12 "3.2.1"]
     [org.apache.spark/spark-sql_2.12 "3.2.1"]]}

   :uberjar
   {:target-path "target/uberjar"
    :uberjar-name "sparkplug-repl.jar"
    :aot :all}})
