(defproject amperity/sparkplug-ml "0.1.6-SNAPSHOT"
  :description "Clojure API for Apache Spark Machine Learning"
  :url "https://github.com/amperity/sparkplug"
  :scm {:dir ".."}
  :license {:name "Apache License 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0"}

  :monolith/inherit true

  :dependencies
  [[org.clojure/clojure "1.10.1"]
   [amperity/sparkplug-core "0.1.6-SNAPSHOT"]]

  :profiles
  {:default
   [:base :system :user :provided :spark-2.4 :dev]

   :spark-2.4
   ^{:pom-scope :provided}
   {:dependencies
    [[org.apache.spark/spark-core_2.12 "2.4.4"]
     [org.apache.spark/spark-mllib_2.12 "2.4.4"]]}

   :spark-3.0
   ^{:pom-scope :provided}
   {:dependencies
    [[org.apache.spark/spark-core_2.12 "3.0.1"]
     [org.apache.spark/spark-mllib_2.12 "3.0.1"]]}})
