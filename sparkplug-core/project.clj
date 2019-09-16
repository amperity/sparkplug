(defproject amperity/sparkplug-core "0.1.0-SNAPSHOT"
  :description "Clojure API for Apache Spark"
  :url "https://github.com/amperity/sparkplug"
  :scm {:dir ".."}
  :license {:name "Apache License 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0"}

  :monolith/inherit true

  :dependencies
  [[org.clojure/clojure "1.10.1"]
   [org.clojure/tools.logging "0.5.0"]
   [clj-time "0.15.2"]
   [com.twitter/carbonite "1.5.0"
    :exclusions [com.esotericsoftware/kryo
                 com.twitter/chill-java]]

   ;; Version conflicts
   [commons-codec "1.10"]
   [com.fasterxml.jackson.core/jackson-core "2.7.9"]
   [com.google.code.findbugs/jsr305 "3.0.2"]
   [org.scala-lang/scala-reflect "2.12.8"]
   [org.slf4j/slf4j-api "1.7.25"]]

  :source-paths ["src/clojure"]
  :java-source-paths ["src/java"]

  :profiles
  {:default
   [:base :system :user :provided :spark-2.4 :dev]

   :dev
   {:jvm-opts ["-server" "-Xmx2g"]}

   :repl
   {:source-paths ["dev"]
    :dependencies
    [[org.clojure/tools.namespace "0.2.11"]]}

   :spark-2.2
   ^{:pom-scope :provided}
   {:dependencies
    [[org.apache.spark/spark-core_2.10 "2.2.3"]]}

   :spark-2.3
   ^{:pom-scope :provided}
   {:dependencies
    [[org.apache.spark/spark-core_2.11 "2.3.4"]]}

   :spark-2.4
   ^{:pom-scope :provided}
   {:dependencies
    [[org.apache.spark/spark-core_2.12 "2.4.4"]]}})
