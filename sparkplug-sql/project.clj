(defproject amperity/sparkplug-sql "0.1.0-SNAPSHOT"
  :description "Clojure API for Apache Spark SQL"
  :url "https://github.com/amperity/sparkplug"
  :scm {:dir ".."}
  :license {:name "Apache License 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0"}

  :monolith/inherit true

  :dependencies
  [[org.clojure/clojure "1.10.1"]
   [amperity/sparkplug-core "0.1.0-SNAPSHOT"]]

  :profiles
  {:default
   [:base :system :user :provided :spark-2.4 :dev]

   :spark-2.2
   ^{:pom-scope :provided}
   {:dependencies
    [[org.apache.spark/spark-core_2.10 "2.2.3"]
     [org.apache.spark/spark-sql_2.10 "2.2.3"]]}

   :spark-2.3
   ^{:pom-scope :provided}
   {:dependencies
    [[org.apache.spark/spark-core_2.11 "2.3.4"]
     [org.apache.spark/spark-sql_2.11 "2.3.4"]]}

   :spark-2.4
   ^{:pom-scope :provided}
   {:dependencies
    [[org.apache.spark/spark-core_2.12 "2.4.4"]
     [org.apache.spark/spark-sql_2.12 "2.4.4"]]}})
