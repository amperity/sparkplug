(ns sparkplug.repl.main
  (:gen-class)
  (:require
    [clojure.java.io :as io]
    [clojure.tools.logging :as log]
    [nrepl.middleware :as middleware]
    [nrepl.middleware.session :as mw-session]
    [nrepl.server :as server]
    [sparkplug.config :as conf]
    [sparkplug.context :as ctx]
    [sparkplug.core :as spark]
    [whidbey.repl :as whidbey]))


(def whidbey-opts
  {:width 200
   :namespace-maps true
   :color-scheme {:nil [:blue]}
   :tag-types {java.lang.Class {'java/class #(symbol (.getName ^Class %))}
               java.time.Instant {'inst str}}})



;; ## REPL Middleware

(def repl-ns 'sparkplug.repl.work)


(defn wrap-repl-init
  "Middleware constructor which ensures the admin-repl system namespace is
  loaded and available before configuring the new session to use it."
  [handler]
  (with-local-vars [sentinel nil]
    (fn [{:keys [session] :as msg}]
      (when-not (@session sentinel)
        (swap! session assoc
               #'*ns*
               (try
                 (require repl-ns)
                 (create-ns repl-ns)
                 (catch Throwable t
                   (log/error t "Failed to switch to repl-ns" repl-ns)
                   (create-ns 'user)))
               sentinel true))
      (handler msg))))


(middleware/set-descriptor!
  #'wrap-repl-init
  {:requires #{#'mw-session/session}
   :expects #{"eval"}})



;; ## Spark Lifecycle

(defn- initialize-context!
  "Construct a new Spark context and intern it in the repl namespace."
  [master]
  (require repl-ns)
  (let [ctx (-> (conf/spark-conf)
                (conf/master master)
                (conf/app-name "sparkplug-repl")
                (conf/jars ["sparkplug-repl.jar"])
                (ctx/spark-context))]
    (intern repl-ns 'spark-context ctx)))


(defn- stop-context!
  "Stop the running Spark context, if any."
  []
  (let [ctx-var (ns-resolve repl-ns 'spark-context)]
    (when-let [ctx (and ctx-var @ctx-var)]
      (ctx/stop! ctx))))



;; ## Main Entry

(def nrepl-server nil)
(def exit-promise (promise))


(defn -main
  "Main entry point for launching the nREPL server."
  [& args]
  (let [master (or (System/getenv "SPARKPLUG_REPL_MASTER")
                   "local[*]")
        port (-> (System/getenv "SPARKPLUG_REPL_PORT")
                 (or "8765")
                 (Integer/parseInt))]
    (try
      (whidbey/init! whidbey-opts)
      (catch Exception ex
        (log/warn ex "Failed to initialize whidbey middleware!")))
    (try
      (log/info "Initializing Spark context...")
      (require repl-ns)
      (initialize-context! master)
      (catch Exception ex
        (log/error ex "Failed to initialize Spark context!")
        (System/exit 10)))
    (log/info "Starting nrepl server on port:" port)
    (let [server (server/start-server
                   :bind "0.0.0.0"
                   :port port
                   :handler (server/default-handler #'wrap-repl-init))]
      (alter-var-root #'nrepl-server (constantly server)))
    @exit-promise
    (log/info "Stopping Spark context...")
    (stop-context!)
    (log/info "Stopping nrepl server...")
    (server/stop-server nrepl-server)
    (System/exit 0)))
