(defproject amperity/sparkplug "1.0.1-SNAPSHOT"
  :description "Clojure API for Apache Spark"
  :url "https://github.com/amperity/sparkplug"
  :license {:name "Apache License 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0"}

  :deploy-branches ["master"]
  :pedantic? :warn
  :deploy-repositories {"releases" {:url "https://repo.clojars.org"}}

  :plugins
  [[lein-cloverage "1.2.2"]
   [lein-monolith "1.7.0"]]

  :dependencies
  [[org.clojure/clojure "1.10.3"]
   [amperity/sparkplug-core "1.0.1-SNAPSHOT"]]

  :profiles
  {:dev
   {:dependencies
    [[org.clojure/test.check "1.1.1"]]}}

  :monolith
  {:project-dirs ["sparkplug-core"
                  "sparkplug-repl"]
   :inherit [:deploy-branches
             :pedantic?]})
