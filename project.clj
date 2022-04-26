(defproject amperity/sparkplug "0.1.9"
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
   [amperity/sparkplug-core "0.1.9"]
   [amperity/sparkplug-sql "0.1.9"]]

  :profiles
  {:dev
   {:dependencies
    [[org.clojure/test.check "1.0.0"]]}}

  :monolith
  {:project-dirs ["sparkplug-core"
                  "sparkplug-sql"
                  "sparkplug-repl"]
   :inherit [:deploy-branches
             :pedantic?]})
