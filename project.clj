(defproject amperity/sparkplug "0.1.0-SNAPSHOT"
  :description "Clojure API for Apache Spark"
  :url "https://github.com/amperity/sparkplug"
  :license {:name "Apache License 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0"}

  :deploy-branches ["master"]
  :pedantic? :warn

  :plugins
  [[lein-cloverage "1.1.1"]
   [lein-monolith "1.2.2"]]

  :dependencies
  [[org.clojure/clojure "1.10.1"]
   [amperity/sparkplug-core "0.1.0-SNAPSHOT"]
   [amperity/sparkplug-sql "0.1.0-SNAPSHOT"]
   [amperity/sparkplug-ml "0.1.0-SNAPSHOT"]]

  :monolith
  {:project-dirs ["sparkplug-core"
                  "sparkplug-sql"
                  "sparkplug-ml"]
   :inherit [:deploy-branches
             :pedantic?]})
