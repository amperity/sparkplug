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
    [clojure.string :as str])
  (:import
    (com.esotericsoftware.kryo
      Kryo
      KryoSerializable
      Serializer)
    java.io.File
    (java.util.jar
      JarEntry
      JarFile)))


;; ## Registry Search

(defn- relative-suffix
  "Return the suffix in `b` if it is prefixed by `a`."
  [a b]
  (let [a (str a)
        b (str b)]
    (when (str/starts-with? b a)
      (subs b (count a)))))


(defn- read-dir-file
  "Read a file from the given directory."
  [^File dir path]
  {:path (str dir)
   :name path
   :content (slurp (io/file dir path))})


(defn- find-dir-files
  "Find all files in the given directory matching the prefix."
  [prefix ^File dir]
  (->> (file-seq dir)
       (keep (partial relative-suffix (str dir "/")))
       (filter #(and (str/starts-with? % prefix)
                     (str/ends-with? % ".conf")))
       (sort)
       (map (partial read-dir-file dir))))


(defn- read-jar-entry
  "Read an entry in the given jar. Returns a map with `:jar`, `:path`, and
  `:content` if the entry is in the jar."
  [^JarFile jar entry-name]
  (when-let [entry (.getEntry jar entry-name)]
    {:path (.getName jar)
     :name entry-name
     :content (slurp (.getInputStream jar entry))}))


(defn- find-jar-entries
  "Find all entries in the given JAR file matching the prefix."
  [prefix ^JarFile jar]
  (->> (classpath/filenames-in-jar jar)
       (filter #(and (str/starts-with? % prefix)
                     (str/ends-with? % ".conf")))
       (sort)
       (keep (partial read-jar-entry jar))))


(defn- find-classpath-files
  "Find all files on the classpath with the given prefix."
  [prefix]
  (concat (mapcat (partial find-dir-files prefix)
                  (classpath/classpath-directories))
          (mapcat (partial find-jar-entries prefix)
                  (classpath/classpath-jarfiles))))


(defn- classpath-registries
  "Return a sequence of registry file maps from the classpath. Returns a
  single entry per distinct config name."
  []
  (->>
    (find-classpath-files "sparkplug/kryo/registry/")
    (map (juxt :name identity))
    (reverse)
    (into (sorted-map))
    (vals)))



;; ## Configuration Hook

(defn configure!
  "Configure the given Kryo instance by loading registries from the classpath."
  [^Kryo kryo]
  ;; TODO: implement
  nil)
