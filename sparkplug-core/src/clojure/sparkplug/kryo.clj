(ns sparkplug.kryo
  "Functions for managing object serialization with Kryo.

  To configure a new Kryo instance, this class looks up all files named
  `sparkplug/kryo/registry.conf` on the classpath. The files are read in sorted
  order, one line at a time. Each line should be tab-separated and begin with
  the desired action:

  - `require        {{namespace}}`
    Require a namespace to load code or for other side effects.
  - `register       {{class}}`
    Register the named class with default serialization.
  - `register       {{class}}     {{serializer-fn}}`
    Register the named class with the given serializer. The named function will
    be resolved and called with no arguments to return a `Serializer` instance.
  - `configure      {{config-fn}}`
    Resolve the named function and call it on the Kryo instance to directly
    configure it.

  Blank lines or lines beginning with a hash (#) are ignored.

  For types which are supported out of the box, the following links may be helpful:
  https://github.com/apache/spark/blob/v2.4.4/core/src/main/scala/org/apache/spark/serializer/KryoSerializer.scala
  https://github.com/twitter/chill/blob/v0.9.3/chill-java/src/main/java/com/twitter/chill/java/PackageRegistrar.java
  https://github.com/twitter/chill/blob/v0.9.3/chill-scala/src/main/scala/com/twitter/chill/ScalaKryoInstantiator.scala#L196

  For general Kryo info:
  https://github.com/EsotericSoftware/kryo"
  (:require
    [clojure.java.classpath :as classpath]
    [clojure.java.io :as io]
    [clojure.string :as str]
    [clojure.tools.logging :as log])
  (:import
    (com.esotericsoftware.kryo
      Kryo
      KryoSerializable
      Serializer)
    java.io.File
    (java.util.jar
      JarEntry
      JarFile)))


(def ^:const registry-prefix
  "SparkPlug registry files must be available under this directory path."
  "sparkplug/kryo/registry")


(def ^:const registry-extension
  "SparkPlug registry file extension."
  ".conf")



;; ## Registry Search

(defn- registry-path?
  "True if the given path is a valid registry file name."
  [path]
  (and (str/starts-with? path registry-prefix)
       (str/ends-with? path registry-extension)))


(defn- relative-suffix
  "Return the suffix in `b` if it is prefixed by `a`."
  [a b]
  (let [a (str a "/")
        b (str b)]
    (when (str/starts-with? b a)
      (subs b (count a)))))


(defn- read-dir-file
  "Read a file from the given directory. Returns a map of registry data."
  [^File dir path]
  (let [file (io/file dir path)]
    (log/debug "Reading registry configuration from file" (str file))
    {:path (str dir)
     :name path
     :text (slurp file)}))


(defn- find-dir-files
  "Find all files in the given directory matching the registry prefix."
  [^File dir]
  (->> (file-seq dir)
       (keep (partial relative-suffix dir))
       (filter registry-path?)
       (sort)
       (map (partial read-dir-file dir))))


(defn- read-jar-entry
  "Read an entry in the given jar. Returns a map of registry data if the entry
  is in the jar."
  [^JarFile jar entry-name]
  (when-let [entry (.getEntry jar entry-name)]
    (log/debugf "Reading registry configuration from JAR entry %s!%s"
               (.getName jar) entry-name)
    {:path (.getName jar)
     :name entry-name
     :text (slurp (.getInputStream jar entry))}))


(defn- find-jar-entries
  "Find all entries in the given JAR file matching the registry prefix."
  [^JarFile jar]
  (->> (classpath/filenames-in-jar jar)
       (filter registry-path?)
       (sort)
       (keep (partial read-jar-entry jar))))


(defn- find-classpath-files
  "Find all config files on the classpath within the registry prefix."
  []
  (concat (mapcat find-dir-files (classpath/classpath-directories))
          (mapcat find-jar-entries (classpath/classpath-jarfiles))))


(defn- classpath-registries
  "Return a sequence of registry file maps from the classpath. Returns a sorted
  sequence with a single entry per distinct config name. Files earlier on the
  classpath will take precedence."
  []
  (->>
    (find-classpath-files)
    (map (juxt :name identity))
    (reverse)
    (into (sorted-map))
    (vals)))



;; ## Registry Files

(defn- parse-registry-line
  "Parse a line from a registry file. Returns a map of information with the
  given line number as `:line`, an action `:type` keyword, and any remaining
  `:args` as a sequence of strings. Returns nil if the line is blank or a
  comment."
  [line-number line]
  (when-not (or (str/blank? line)
                (str/starts-with? line "#"))
    (let [[action-type & args] (str/split line #"\t")]
      {:line line-number
       :type (keyword action-type)
       :args (vec args)})))


(defn- run-require-action!
  "Perform a `require` action from a registry config."
  [args]
  (when-not (= 1 (count args))
    (throw (ex-info (str "require action takes exactly one argument, not "
                         (count args))
                    {:type ::bad-action})))
  (when (str/includes? (first args) "/")
    (throw (ex-info "require action argument should not be namespaced"
                    {:type ::bad-action})))
  (let [ns-sym (symbol (first args))]
    (log/debug "Requiring namespace" ns-sym)
    (require ns-sym)))


(defn- run-register-action!
  "Perform a `register` action from a registry config."
  [^Kryo kryo args]
  (when-not (<= 1 (count args) 2)
    (throw (ex-info (str "register action takes one or two arguments, not "
                         (count args))
                    {:type ::bad-action})))
  (when (and (second args) (not (str/includes? (second args) "/")))
    (throw (ex-info "register action serializer should be a namespaced symbol"
                    {:type ::bad-action})))
  (let [[class-name serializer-name] args]
    (log/debugf "Registering class %s with %s serializer"
                class-name
                (or serializer-name "default"))
    ;; Load the class to register.
    (let [clazz (Class/forName class-name)]
      (if serializer-name
        ;; Resolve the named function to construct a new serializer instance.
        (if-let [constructor (requiring-resolve (symbol serializer-name))]
          (.register kryo clazz (constructor))
          (throw (ex-info (str "Could not resolve serializer constructor function "
                               serializer-name)
                          {:type ::bad-action})))
        ;; No serializer, register with defaults.
        (.register kryo clazz)))))


(defn- run-configure-action!
  "Perform a `configure` action from a registry config."
  [^Kryo kryo args]
  (when-not (= 1 (count args)))
    (throw (ex-info (str "configure action takes exactly one argument, not "
                         (count args))
                    {:type ::bad-action}))
  (when-not (str/includes? (first args) "/")
    (throw (ex-info "configure action function should be a namespaced symbol"
                    {:type ::bad-action})))
  (let [var-name (symbol (first args))]
    (log/debug "Configuring Kryo with function" var-name)
    (if-let [configure (requiring-resolve var-name)]
      (configure kryo)
      (throw (ex-info (str "Could not resolve configuration function "
                           var-name)
                      {:type ::bad-action})))))


(defn- run-action!
  "Take the configuration `action` as read from the given `registry`.
  Dispatches by action type."
  [kryo registry action]
  (let [{:keys [path name text]} registry
        {:keys [line type args]} action]
    (try
      (case type
        :require
        (run-require-action! args)

        :register
        (run-register-action! kryo args)

        :configure
        (run-configure-action! kryo args)

        (throw (ex-info (str "Unsupported registry action " (pr-str type))
                        {:type ::bad-action})))
      (catch Exception ex
        (let [message (format "Failed to perform %s action on line %s of %s in %s"
                              (name type) line name path)
              cause (when (not= ::bad-action (:type (ex-data ex)))
                      ex)]
          (log/error message (.getMessage ex))
          (throw (ex-info (str message ": " (.getMessage ex))
                          {:path path
                           :name name
                           :line line
                           :type type
                           :args args}
                          cause)))))))


(defn- load-registry!
  "Process the given registry file map and enact the contained actions."
  [kryo registry]
  (log/debugf "Loading registry %s in %s" (:name registry) (:path registry))
  (->>
    (:content registry)
    (str/split-lines)
    (map-indexed parse-registry-line)
    (run! (partial run-action! kryo registry))))



;; ## Configuration Hook

(defn configure!
  "Configure the given Kryo instance by loading registries from the classpath."
  [^Kryo kryo]
  (run! (partial load-registry! kryo) (classpath-registries)))
