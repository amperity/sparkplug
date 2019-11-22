(ns sparkplug.sql.listener.events
  (:require
    [sparkplug.listener :as listener]
    [sparkplug.scala :as scala])
  (:import
    org.apache.spark.sql.execution.metric.SQLMetricInfo
    org.apache.spark.sql.execution.SparkPlanInfo
    (org.apache.spark.sql.execution.ui
      SparkListenerSQLExecutionEnd
      SparkListenerSQLExecutionStart
      SparkListenerDriverAccumUpdates)))


(defn- parse-metric-info
  [^SQLMetricInfo metric-info]
  {:name (.name metric-info)
   :accumulator-id (.accumulatorId metric-info)
   :metric-type (.metricType metric-info)})


(defn- parse-plan-info
  [^SparkPlanInfo plan-info]
  {:node-name (.nodeName plan-info)
   :simple-string (.simpleString plan-info)
   :children (->> (.children plan-info)
                  (scala/scala-seq->clj-seq)
                  (into [] (map parse-plan-info)))
   :metadata (->> (.metadata plan-info)
                  (scala/scala-seq->clj-seq)
                  (into {} (map scala/from-pair)))
   :metrics (->> (.metrics plan-info)
                 (scala/scala-seq->clj-seq)
                 (into [] (map parse-metric-info)))})


(defmethod listener/parse-event SparkListenerSQLExecutionStart
  [^SparkListenerSQLExecutionStart event]
  {:type :sql-execution-start
   :execution-id (.executionId event)
   :description (.description event)
   :details (.details event)
   :spark-plan-info (parse-plan-info (.sparkPlanInfo event))
   :time (.time event)})


(defmethod listener/parse-event SparkListenerSQLExecutionEnd
  [^SparkListenerSQLExecutionEnd event]
  {:type :sql-execution-end
   :execution-id (.executionId event)
   :time (.time event)})


(defmethod listener/parse-event SparkListenerDriverAccumUpdates
  [^SparkListenerDriverAccumUpdates event]
  {:type :sql-metric-update
   :execution-id (.executionId event)
   :accum-updates (->> (.accumUpdates event)
                       (scala/scala-seq->clj-seq)
                       (into {} (map scala/from-pair)))})
