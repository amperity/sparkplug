(ns sparkplug.kryo
  "Functions for managing object serialization with Kryo.

  To configure a new Kryo instance, this class looks up all files named
  `sparkplug/kryo/registry.conf` on the classpath. The files are read in sorted
  order, one line at a time. Each line should be tab-separated and begin with
  the desired action:

  - `require        {{namespace}}`
    Require a namespace to load code or for other side effects.
  - `register       {{class}}`
    Register the named class with default serialization. The class name may be
    suffixed with `[]` pairs to indicate array class types.
  - `register       {{class}}     {{serializer}}`
    Register the named class with the given serializer. The serializer may
    either be the name of a class to instantiate with the default constructor,
    or a qualified function var be resolved and called with no arguments to
    return a `Serializer` instance.
  - `configure      {{config-fn}}`
    Resolve the named function and call it on the Kryo instance to directly
    configure it.

  Blank lines or lines beginning with a hash (#) are ignored."
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


;; ## Registry Files

(def ^:const registry-prefix
  "SparkPlug registry files must be available under this directory path."
  "sparkplug/kryo/registry")


(def ^:const registry-extension
  "SparkPlug registry file extension."
  ".conf")


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


(defn- parse-registry-actions
  "Parse the text content of the given registry data map. Returns an updated map
  with `:text` removed and `:actions` set to the parsed lines."
  [registry]
  (let [actions (->>
                  (:text registry)
                  (str/split-lines)
                  (map-indexed parse-registry-line)
                  (remove nil?)
                  (vec))]
    (-> registry
        (assoc :actions actions)
        (dissoc :text))))


(defn classpath-registries
  "Return a sequence of registry file maps from the classpath. Returns a sorted
  sequence with a single entry per distinct config name. Files earlier on the
  classpath will take precedence."
  []
  (->>
    (find-classpath-files)
    (map (juxt :name parse-registry-actions))
    (reverse)
    (into (sorted-map))
    (vals)))



;; ## Registry Actions

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


(defn- convert-array-class
  "Determine the base class and number of nested arrays for a class name like
  `String[][]`. Returns a rewritten string in a form that the classloader will
  understand like `[[LString;`."
  [class-name]
  (loop [class-name class-name
         arrays 0]
    (if (str/ends-with? class-name "[]")
      (recur (subs class-name (- (count class-name) 2))
             (inc arrays))
      (if (zero? arrays)
        class-name
        (str (str/join (repeat arrays \[))
             "L" class-name ";")))))


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
    (let [target-class (Class/forName (convert-array-class class-name))]
      (if serializer-name
        (if (str/includes? serializer-name "/")
          ;; Resolve the named function to construct a new serializer instance.
          (if-let [constructor (requiring-resolve (symbol serializer-name))]
            (.register kryo target-class (constructor))
            (throw (ex-info (str "Could not resolve serializer constructor function "
                                 serializer-name)
                            {:type ::bad-action})))
          ;; Assume the serializer is a class name and construct an instance.
          (let [serializer-class (Class/forName serializer-name)
                serializer (.newInstance serializer-class)]
            (.register kryo target-class serializer)))
        ;; No serializer, register with defaults.
        (.register kryo target-class)))))


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


(defn load-registry!
  "Process the given registry file map and enact the contained actions."
  [kryo registry]
  (log/debugf "Loading registry %s in %s" (:name registry) (:path registry))
  (run! (partial run-action! kryo registry) (:actions registry)))


(defn configure!
  "Configure the given Kryo instance by loading registries from the classpath."
  [kryo]
  (run! (partial load-registry! kryo) (classpath-registries)))
