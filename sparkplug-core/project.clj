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
   [:base :system :user :provided :spark-3.5 :dev]

   :dev
   {:dependencies
    [[org.clojure/test.check "1.1.1"]
     [org.slf4j/slf4j-api "2.0.7"]
     [org.slf4j/slf4j-simple "2.0.7"]]
    :jvm-opts ["-Xmx2g"
               "-XX:-OmitStackTraceInFastThrow"
               "-Dorg.slf4j.simpleLogger.defaultLogLevel=warn"
               "-Dorg.slf4j.simpleLogger.log.org.apache=warn"]}

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
    [[org.apache.spark/spark-core_2.12 "3.5.1"
      :exclusions [org.apache.logging.log4j/log4j-slf4j2-impl]]

     ;; Conflict resolution
     [com.fasterxml.jackson.core/jackson-core "2.15.2"]
     [com.google.code.findbugs/jsr305 "3.0.2"]]}})
