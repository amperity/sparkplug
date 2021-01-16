(defproject amperity/sparkplug-sql "0.1.7"
  :description "Clojure API for Apache Spark SQL"
  :url "https://github.com/amperity/sparkplug"
  :scm {:dir ".."}
  :license {:name "Apache License 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0"}

  :monolith/inherit true

  :dependencies
  [[org.clojure/clojure "1.10.1"]
   [amperity/sparkplug-core "0.1.7"]]

  :source-paths ["src/clojure"]
  :java-source-paths ["src/java"]

  :profiles
  {:default
   [:base :system :user :provided :spark-2.4 :dev]

   :repl
   {:source-paths ["dev"]}

   :spark-2.4
   ^{:pom-scope :provided}
   {:dependencies
    [[org.apache.spark/spark-core_2.12 "2.4.4"]
     [org.apache.spark/spark-sql_2.12 "2.4.4"]]}

   :spark-3.0
   ^{:pom-scope :provided}
   {:dependencies
    [[org.apache.spark/spark-core_2.12 "3.0.1"]
     [org.apache.spark/spark-sql_2.12 "3.0.1"]]}})
