(ns sparkplug.kryo
  "Functions for managing object serialization with Kryo.

  To configure a new Kryo instance, this class looks up all resources in
  directories named `sparkplug/kryo/registry/` on the classpath. The files are
  read in sorted order, one line at a time. Each line should be tab-separated
  and begin with the desired action:

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
    (clojure.lang
      BigInt
      IPersistentMap
      IPersistentSet
      IPersistentVector
      ISeq
      Keyword
      Named
      PersistentTreeMap
      PersistentTreeSet
      Ratio
      StringSeq
      Symbol
      Var)
    (com.esotericsoftware.kryo
      Kryo
      Serializer)
    (com.esotericsoftware.kryo.io
      Input
      Output)
    java.io.File
    java.math.BigInteger
    (java.util.jar
      JarEntry
      JarFile)
    org.objenesis.strategy.StdInstantiatorStrategy))


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

(defn- load-require-action
  "Prepare a `require` action from a registry. Requires the namespace and
  returns nil."
  [args]
  ;; Check arguments.
  (when-not (= 1 (count args))
    (throw (ex-info (str "require action takes exactly one argument, not "
                         (count args))
                    {:type ::bad-action})))
  (when (str/includes? (first args) "/")
    (throw (ex-info "require action argument should not be namespaced"
                    {:type ::bad-action})))
  ;; Require the namespace code.
  (let [ns-sym (symbol (first args))]
    (log/debug "Requiring namespace" ns-sym)
    (require ns-sym))
  ;; Nothing to do per-kryo instance afterwards.
  nil)


(defn- convert-array-class
  "Determine the base class and number of nested arrays for a class name like
  `String[][]`. Returns a rewritten string in a form that the classloader will
  understand like `[[LString;`."
  [class-name]
  (loop [class-name class-name
         arrays 0]
    (if (str/ends-with? class-name "[]")
      (recur (subs class-name 0 (- (count class-name) 2))
             (inc arrays))
      (if (zero? arrays)
        class-name
        (str (str/join (repeat arrays \[))
             "L" class-name ";")))))


(defn- load-register-action
  "Prepare a `register` action from a registry at load-time. Loads the class to
  register and any serialzer and returns a function which will register the
  class with a Kryo instance."
  [args]
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
            (fn register
              [^Kryo kryo]
              (let [serializer ^Serializer (constructor)]
                (.register kryo target-class serializer)))
            (throw (ex-info (str "Could not resolve serializer constructor function "
                                 serializer-name)
                            {:type ::bad-action})))
          ;; Assume the serializer is a class name and construct an instance.
          (let [serializer-class (Class/forName serializer-name)]
            (fn register
              [^Kryo kryo]
              (let [serializer ^Serializer (.newInstance serializer-class)]
                (.register kryo target-class serializer)))))
        ;; No serializer, register with defaults.
        (fn register
          [^Kryo kryo]
          (.register kryo target-class))))))


(defn- load-configure-action
  "Prepare a `configure` action from a registry at load-time. Resolves the
  configuration function and returns it."
  [args]
  (when-not (= 1 (count args))
    (throw (ex-info (str "configure action takes exactly one argument, not "
                         (count args))
                    {:type ::bad-action})))
  (when-not (str/includes? (first args) "/")
    (throw (ex-info "configure action function should be a namespaced symbol"
                    {:type ::bad-action})))
  (let [var-name (symbol (first args))]
    (log/debug "Configuring Kryo with function" var-name)
    (or (requiring-resolve var-name)
        (throw (ex-info (str "Could not resolve configuration function "
                             var-name)
                        {:type ::bad-action})))))


(defn- load-action
  "Load the configuration `action` as read from the given `registry`.
  Dispatches on action type."
  [registry action]
  (let [{:keys [path name text]} registry
        {:keys [line type args]} action]
    (try
      (case type
        :require
        (load-require-action args)

        :register
        (load-register-action args)

        :configure
        (load-configure-action args)

        (throw (ex-info (str "Unsupported registry action " (pr-str type))
                        {:type ::bad-action})))
      (catch Exception ex
        (let [message (format "Failed to load %s action on line %s of %s in %s"
                              (clojure.core/name type) line name path)
              cause (when (not= ::bad-action (:type (ex-data ex)))
                      ex)]
          (log/error message (ex-message ex))
          (throw (ex-info (str message ": " (ex-message ex))
                          {:path path
                           :name name
                           :line line
                           :type type
                           :args args}
                          cause)))))))


(defn- load-registry
  "Process the given registry file map and returns a sequence of all
  loaded configuration functions."
  [registry]
  (log/debugf "Loading registry %s in %s" (:name registry) (:path registry))
  (into []
        (keep (partial load-action registry))
        (:actions registry)))


(defn load-configuration
  "Walk the classpath and load configuration actions from all discovered
  registries. Returns a function which can be called on a Kryo serializer to
  configure it."
  []
  (let [actions (into [] (mapcat load-registry) (classpath-registries))]
    (fn configure!
      [^Kryo kryo]
      (.setInstantiatorStrategy kryo (StdInstantiatorStrategy.))
      (doseq [f actions]
        (f kryo)))))


(defn initialize
  "Creates a new Kryo instance and configures it with classpath registry
  actions."
  ^Kryo
  []
  (let [configure! (load-configuration)]
    (doto (Kryo.)
      (configure!))))


;; ## Serialization Logic

;; For types that are already registered with efficient serializers, see:
;; https://github.com/EsotericSoftware/kryo/blob/master/src/com/esotericsoftware/kryo/Kryo.java
;; https://github.com/twitter/chill/blob/v0.9.3/chill-java/src/main/java/com/twitter/chill/java/PackageRegistrar.java
;; https://github.com/twitter/chill/blob/v0.9.3/chill-scala/src/main/scala/com/twitter/chill/ScalaKryoInstantiator.scala

(defmacro defserializer
  "Define a new constructor for a Kryo Serializer with the given `write` and
  `read` method implementations."
  [name-sym class-sym immutable? & body]
  ;; TODO: a spec for this macro would be better than these assertions
  {:pre [(symbol? name-sym)
         (symbol? class-sym)
         (boolean? immutable?)
         (= 2 (count body))
         (every? list? body)
         (= #{'read 'write} (set (map first body)))]}
  (let [tagged #(vary-meta %1 assoc :tag (if (instance? Class %2)
                                           (.getName ^Class %2)
                                           (str %2)))
        name-sym (tagged name-sym Serializer)
        body-methods (into {} (map (juxt first identity)) body)
        write-form (get body-methods 'write)
        read-form (get body-methods 'read)]
    ^:cljfmt/ignore
    `(defn ~name-sym
       ~(str "Construct a new Kryo serializer for " class-sym " values.")
       []
       (proxy [Serializer] [false ~immutable?]

         (write
           ~(let [[kryo-sym output-sym value-sym] (second write-form)]
              [(tagged kryo-sym Kryo)
               (tagged output-sym Output)
               (tagged value-sym class-sym)])
           ~@(nnext write-form))

         (read
           ~(let [[kryo-sym input-sym target-sym] (second read-form)]
              [(tagged kryo-sym Kryo)
               (tagged input-sym Input)
               (tagged target-sym Class)])
           ~@(nnext read-form))))))


;; ### Core Serializers

(defserializer ident-serializer
  Named true

  (write
    [kryo output value]
    (let [named-str (if (keyword? value)
                      (subs (str value) 1)
                      (str value))]
      (.writeString output named-str)))

  (read
    [kryo input target-class]
    (let [named-str (.readString input)]
      (if (identical? Keyword target-class)
        (keyword named-str)
        (symbol named-str)))))


(defn- write-biginteger
  "Write a BigInteger to the Kryo output."
  [^Output output ^BigInteger value]
  (let [int-bytes (.toByteArray value)]
    (.writeVarInt output (alength int-bytes) true)
    (.writeBytes output int-bytes)))


(defn- read-biginteger
  "Read a BigInteger value from the Kryo input."
  [^Input input]
  (let [length (.readVarInt input true)
        int-bytes (.readBytes input length)]
    (BigInteger. int-bytes)))


(defserializer bigint-serializer
  BigInt true

  (write
    [kryo output value]
    (write-biginteger output (biginteger value)))

  (read
    [kryo input _]
    (bigint (read-biginteger input))))


(defserializer ratio-serializer
  Ratio true

  (write
    [kryo output value]
    (write-biginteger output (numerator value))
    (write-biginteger output (denominator value)))

  (read
    [kryo input _]
    (/ (read-biginteger input)
       (read-biginteger input))))


(defserializer var-serializer
  Var false

  (write
    [kryo output value]
    (.writeString output (str (symbol value))))

  (read
    [kryo input _]
    (let [var-sym (symbol (.readString input))]
      (requiring-resolve var-sym))))


;; ### Sequence Serializers

(defn- write-sequence
  "Write a sequence of values to the Kryo output."
  [^Kryo kryo ^Output output coll]
  (.writeVarInt output (count coll) true)
  (doseq [x coll]
    (.writeClassAndObject kryo output x)))


(defn- read-sequence
  "Read a lazy sequence of values from the Kryo output."
  [^Kryo kryo ^Input input]
  (let [length (.readVarInt input true)]
    (repeatedly length #(.readClassAndObject kryo input))))


(defserializer sequence-serializer
  ISeq true

  (write
    [kryo output coll]
    (write-sequence kryo output coll))

  (read
    [kryo input _]
    (apply list (read-sequence kryo input))))


(defserializer vector-serializer
  IPersistentVector true

  (write
    [kryo output coll]
    (write-sequence kryo output coll))

  (read
    [kryo input _]
    (into [] (read-sequence kryo input))))


(defserializer string-seq-serializer
  StringSeq true

  (write
    [kryo output coll]
    (.writeString output (str/join coll)))

  (read
    [kryo input _]
    (seq (.readString input))))


;; ### Set Serializers

(defserializer set-serializer
  IPersistentSet true

  (write
    [kryo output coll]
    (write-sequence kryo output coll))

  (read
    [kryo input _]
    (into #{} (read-sequence kryo input))))


(defserializer ordered-set-serializer
  PersistentTreeSet true

  (write
    [kryo output coll]
    (.writeClassAndObject kryo output (.comparator coll))
    (write-sequence kryo output coll))

  (read
    [kryo input _]
    (let [cmp (.readClassAndObject kryo input)]
      (into (sorted-set-by cmp) (read-sequence kryo input)))))


;; ### Map Serializers

(defn- write-kvs
  "Write a sequence of key/value pairs to the Kryo output."
  [^Kryo kryo ^Output output coll]
  (.writeVarInt output (count coll) true)
  (doseq [[k v] coll]
    (.writeClassAndObject kryo output k)
    (.writeClassAndObject kryo output v)))


(defn- read-kvs
  "Read a lazy sequence of key/value pairs from the Kryo output."
  [^Kryo kryo ^Input input]
  (let [length (.readVarInt input true)]
    (repeatedly length #(clojure.lang.MapEntry.
                          (.readClassAndObject kryo input)
                          (.readClassAndObject kryo input)))))


(defserializer map-serializer
  IPersistentMap true

  (write
    [kryo output coll]
    (write-kvs kryo output coll))

  (read
    [kryo input _]
    (into {} (read-kvs kryo input))))


(defserializer ordered-map-serializer
  PersistentTreeMap true

  (write
    [kryo output coll]
    (.writeClassAndObject kryo output (.comparator coll))
    (write-kvs kryo output coll))

  (read
    [kryo input _]
    (let [cmp (.readClassAndObject kryo input)]
      (into (sorted-set-by cmp) (read-kvs kryo input)))))



;; ## Serialization Utilities

;; These are handy for tests and repl usage, but aren't actually used directly
;; by the library.

(defn encode
  "Serialize the given object into a byte arary using the Kryo codec."
  ^bytes
  [^Kryo kryo obj]
  (let [output (Output. 512 8192)]
    (.writeClassAndObject kryo output obj)
    (.toBytes output)))


(defn decode
  "Deserialize the given byte array using the Kryo codec."
  [^Kryo kryo ^bytes data]
  (let [input (Input. data)]
    (.readClassAndObject kryo input)))
