(ns sparkplug.listener
  "Functions for creating and working with Spark listeners."
  (:require
    [sparkplug.scala :as scala])
  (:import
    java.util.Properties
    org.apache.spark.api.java.JavaSparkContext
    (org.apache.spark.scheduler
      StageInfo
      SparkListenerJobStart SparkListenerJobEnd JobSucceeded JobFailed SparkListenerTaskStart TaskInfo SparkListenerTaskGettingResult SparkListenerSpeculativeTaskSubmitted SparkListenerTaskEnd)
    org.apache.spark.SparkContext
    sparkplug.listener.ListenerBridge
    (org.apache.spark.storage RDDInfo)
    (org.apache.spark.executor TaskMetrics InputMetrics OutputMetrics ShuffleReadMetrics ShuffleWriteMetrics)))


;; ## Parsing Helpers

(defn- parse-rdd-info
  [^RDDInfo rdd-info]
  {:id (.id rdd-info)
   :name (.name rdd-info)
   :num-partitions (.numPartitions rdd-info)
   :parent-ids (into [] (scala/scala-seq->clj-seq (.parentIds rdd-info)))
   :call-site (.callSite rdd-info)})


(defn- parse-stage-info
  [^StageInfo stage-info]
  (let [submission-time (-> stage-info .submissionTime scala/resolve-option)
        completion-time (-> stage-info .completionTime scala/resolve-option)
        failure-reason (-> stage-info .failureReason scala/resolve-option)]
    (merge
      {:stage-id (.stageId stage-info)
       :name (.name stage-info)
       :num-tasks (.numTasks stage-info)
       :rdd-infos (into []
                        (map parse-rdd-info)
                        (scala/scala-seq->clj-seq (.rddInfos stage-info)))
       :parent-ids (into [] (scala/scala-seq->clj-seq (.parentIds stage-info)))
       :details (.details stage-info)
       :status (.getStatusString stage-info)}
      (when submission-time {:submission-time submission-time})
      (when completion-time {:completion-time completion-time})
      (when failure-reason {:failure-reason failure-reason}))))


(defn- parse-properties
  [^Properties prop]
  (reduce (fn [m [k v]] (assoc m k v)) {} prop))


(defn- parse-task-info
  [^TaskInfo task-info]
  (let [status (case (.status task-info)
                 "GET RESULT" :get-result
                 "RUNNING" :running
                 "FAILED" :failed
                 "SUCCESS" :succeeded
                 "UNKNOWN" :unknown)]
    (merge
      {:task-id (.taskId task-info)
       :index (.index task-info)
       :attempt-number (.attemptNumber task-info)
       :launch-time (.launchTime task-info)
       :executor-id (.executorId task-info)
       :host (.host task-info)
       :speculative? (.speculative task-info)
       :status status}
      (when-not (= status :running)
        {:duration (.duration task-info)}))))


(defn- parse-input-metrics
  [^InputMetrics input-metrics]
  {:bytes-read (.bytesRead input-metrics)
   :records-read (.recordsRead input-metrics)})


(defn- parse-output-metrics
  [^OutputMetrics output-metrics]
  {:bytes-written (.bytesWritten output-metrics)
   :records-written (.recordsWritten output-metrics)})


(defn- parse-shuffle-read-metrics
  [^ShuffleReadMetrics shuffle-read-metrics]
  {:remote-blocks-fetched (.remoteBlocksFetched shuffle-read-metrics)
   :local-blocks-fetched (.localBlocksFetched shuffle-read-metrics)
   :remote-bytes-read (.remoteBytesRead shuffle-read-metrics)
   :remote-bytes-read-to-disk (.remoteBytesReadToDisk shuffle-read-metrics)
   :local-bytes-read (.localBytesRead shuffle-read-metrics)
   :fetch-wait-time (.fetchWaitTime shuffle-read-metrics)
   :records-read (.recordsRead shuffle-read-metrics)
   :total-bytes-read (.totalBytesRead shuffle-read-metrics)
   :total-blocks-fetched (.totalBlocksFetched shuffle-read-metrics)})


(defn- parse-shuffle-write-metrics
  [^ShuffleWriteMetrics shuffle-write-metrics]
  {:bytes-written (.bytesWritten shuffle-write-metrics)
   :records-written (.recordsWritten shuffle-write-metrics)
   :write-time (.writeTime shuffle-write-metrics)})


(defn- parse-task-metrics
  [^TaskMetrics task-metrics]
  {:executor-deserialize-time (.executorDeserializeTime task-metrics)
   :executor-deserialize-cpu-time (.executorDeserializeCpuTime task-metrics)
   :executor-run-time (.executorRunTime task-metrics)
   :executor-cpu-time (.executorCpuTime task-metrics)
   :result-size (.resultSize task-metrics)
   :jvm-gc-time (.jvmGCTime task-metrics)
   :result-serialization-time (.resultSerializationTime task-metrics)
   :memory-bytes-spilled (.memoryBytesSpilled task-metrics)
   :disk-bytes-spilled (.diskBytesSpilled task-metrics)
   :peak-execution-memory (.peakExecutionMemory task-metrics)
   :input-metrics (parse-input-metrics (.inputMetrics task-metrics))
   :output-metrics (parse-output-metrics (.outputMetrics task-metrics))
   :shuffle-read-metrics (parse-shuffle-read-metrics (.shuffleReadMetrics task-metrics))
   :shuffle-write-metrics (parse-shuffle-write-metrics (.shuffleWriteMetrics task-metrics))})


;; ## Event Parsing

(defmulti parse-event
  "Parses an incoming Spark listener event into a clojure map"
  class)


(defmethod parse-event :default [event] nil)


(defmethod parse-event SparkListenerJobStart
  [^SparkListenerJobStart event]
  {:type :job-start
   :job-id (.jobId event)
   :time (.time event)
   :stage-infos (into []
                      (map parse-stage-info)
                      (scala/scala-seq->clj-seq (.stageInfos event)))
   :properties (parse-properties (.properties event))})


(defmethod parse-event SparkListenerJobEnd
  [^SparkListenerJobEnd event]
  (let [job-result (.jobResult event)
        result (case (class job-result)
                 JobSucceeded :succeeded
                 JobFailed :failed)]
    (merge
      {:type :job-end
       :job-id (.jobId event)
       :time (.time event)
       :result result}
      (when (= result :failed)
        {:exception (.exception ^JobFailed job-result)}))))


(defmethod parse-event SparkListenerTaskStart
  [^SparkListenerTaskStart event]
  {:type :task-start
   :stage-id (.stageId event)
   :stage-attempt-id (.stageAttemptId event)
   :task-info (parse-task-info (.taskInfo event))})


(defmethod parse-event SparkListenerTaskGettingResult
  [^SparkListenerTaskGettingResult event]
  {:type :task-getting-result
   :task-info (parse-task-info (.taskInfo event))})


(defmethod parse-event SparkListenerSpeculativeTaskSubmitted
  [^SparkListenerSpeculativeTaskSubmitted event]
  {:type :speculative-task-submitted
   :stage-id (.stageId event)})


(defmethod parse-event SparkListenerTaskEnd
  [^SparkListenerTaskEnd event]
  (merge
    {:type :task-end
     :stage-id (.stageId event)
     :stage-attempt-id (.stageAttemptId event)
     :task-type (.taskType event)
     :reason nil
     :task-info (parse-task-info (.taskInfo event))}
    (when-let [task-metrics (.taskMetrics event)]
      {:task-metrics (parse-task-metrics task-metrics)})))


;; ## Public API

(defprotocol Listener
  (on-event [this event]))


(defn register
  "Registers the given listener to the given spark context. Returns the bridge object
  that was constructed, which is needed to unregister the listener."
  [^JavaSparkContext context listener]
  (let [^SparkContext sc (.sc context)
        listener-fn (fn [event]
                      (when-let [parsed-event (parse-event event)]
                        (on-event listener parsed-event)))
        handle (ListenerBridge. listener-fn)]
    (.addSparkListener sc handle)
    handle))


(defn unregister
  "Unregisters the given lister bridge object from the given spark context."
  [^JavaSparkContext context handle]
  (let [^SparkContext sc (.sc context)]
    (.removeSparkListener sc handle)))
