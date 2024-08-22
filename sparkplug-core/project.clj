(defproject amperity/sparkplug-core "1.0.1-SNAPSHOT"
  :description "Clojure API for Apache Spark"
  :url "https://github.com/amperity/sparkplug"
  :scm {:dir ".."}
  :license {:name "Apache License 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0"}

  :monolith/inherit true

  :dependencies
  [[org.clojure/clojure "1.11.1"]
   [org.clojure/java.classpath "1.0.0"]
   [org.clojure/tools.logging "1.2.4"]]

  :source-paths ["src/clojure"]
  :java-source-paths ["src/java"]

  :profiles
  {:default
   [:base :system :user :provided :spark-3.1 :dev]

   :dev
   {:jvm-opts ["-server" "-Xmx2g"]
    :dependencies
    [[org.clojure/test.check "1.1.1"]]}

   :repl
   {:source-paths ["dev"]
    :aot [sparkplug.bool-repro]
    :dependencies
    [[org.clojure/tools.namespace "1.3.0"]]}

   :test
   {:jvm-opts ["-XX:-OmitStackTraceInFastThrow"]}

   :spark-3.1
   ^{:pom-scope :provided}
   {:dependencies
    [[org.apache.spark/spark-core_2.12 "3.1.3"]]}

   :spark-3.5
   ^{:pom-scope :provided}
   {:dependencies
    [[org.apache.spark/spark-core_2.12 "3.5.1"]]}})
