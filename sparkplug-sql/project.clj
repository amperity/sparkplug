(defproject amperity/sparkplug-sql "1.0.0-SNAPSHOT"
  :description "Clojure API for Apache Spark SQL"
  :url "https://github.com/amperity/sparkplug"
  :scm {:dir ".."}
  :license {:name "Apache License 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0"}

  :monolith/inherit true

  :dependencies
  [[org.clojure/clojure "1.10.3"]
   [amperity/sparkplug-core "1.0.0-SNAPSHOT"]]

  :source-paths ["src/clojure"]
  :java-source-paths ["src/java"]

  :profiles
  {:default
   [:base :system :user :provided :spark-3.1 :dev]

   :repl
   {:source-paths ["dev"]}

   :spark-3.1
   ^{:pom-scope :provided}
   {:dependencies
    [[org.apache.spark/spark-core_2.12 "3.1.3"]
     [org.apache.spark/spark-sql_2.12 "3.1.3"]]}

   :spark-3.2
   ^{:pom-scope :provided}
   {:dependencies
    [[org.apache.spark/spark-core_2.12 "3.2.1"]
     [org.apache.spark/spark-sql_2.12 "3.2.1"]]}})
