(defproject amperity/sparkplug-ml "0.1.9-SNAPSHOT"
  :description "Clojure API for Apache Spark Machine Learning"
  :url "https://github.com/amperity/sparkplug"
  :scm {:dir ".."}
  :license {:name "Apache License 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0"}

  :monolith/inherit true

  :dependencies
  [[org.clojure/clojure "1.10.1"]
   [amperity/sparkplug-core "0.1.8-SNAPSHOT"]]

  :profiles
  {:default
   [:base :system :user :provided :spark-3.1 :dev]

   :spark-3.1
   ^{:pom-scope :provided}
   {:dependencies
    [[org.apache.spark/spark-core_2.12 "3.1.3"]
     [org.apache.spark/spark-mllib_2.12 "3.1.3"]]}

   :spark-3.2
   ^{:pom-scope :provided}
   {:dependencies
    [[org.apache.spark/spark-core_2.12 "3.2.1"]
     [org.apache.spark/spark-mllib_2.12 "3.2.1"]]}})
