(ns dataproc.services.batchrunner
  (:require [immutant.daemons :as daemon]
            [dataproc.config :as config]
            [dataproc.cache.core :as cache]
            [immutant.cache :as dcache]
            [taoensso.timbre :as log]
            [immutant.jobs :as q]))

(declare ^:private bcache)

(defn register-batch
  [name fn]
  ; Note for report - this is so that if the daemon went down, taking the query with it,
  ; they would be in the cache and started when the daemon is started on another node
  (dcache/swap! bcache :batch-list conj (hash-map name fn))
  (println "BATCH-LIST" (get bcache :batch-list))
  (schedule-job name fn))

(defn unregister-batch
  [name]
  ; This is for removing from batch-list
  (cache/swap! bcache :batch-list dissoc (get bcache :batch-list) name)
  (println "BATCH-LIST" (get bcache :batch-list))
  (q/unschedule name))
  
(defn add-job-to-running-list
  [name]
  (dcache/swap! bcache :running-batch-list conj name))

(defn remove-job-from-running-list
  [name]
  (dcache/swap! bcache :running-batch-list disj name))
 
  ; start-job
  ; add name to :running-batch-list
  ; when it ends remove from :running-batch-list
(defn job-wrapper
  "name - the name of the job
   fn   - the function to be executed

   Returns a function that will be called by the Quartz scheduler.
   If running-batch-list contains the job name, it is currently
   being executed and so will do nothing. Otherwise is adds the job
   to the running list, execute the function then remove the job from
   the running list"
  [name fn]
  (fn []
    (when-not (contains? (get bcache :running-batch-list) name)
      (do
        (add-job-to-running-list name)
        (fn)
        (remove-job-from-running-list name)))))

(defn schedule-job
  [name fn]
  (log/info "Scheduling batch query job:" name ", running every 5 minutes")
  (q/schedule name (job-wrapper name fn) :every [5 :minutes]))
  
(defn init
  []
  (def bcache (cache/create-cache "batchrunner"))
  (dcache/put bcache :batch-list #{}))

(defrecord BatchRunner []
  daemon/Daemon
  (start [_]
    (dcache/put bcache :running-batch-list {})
    ; Schedule batches from cache
    (doseq [job (get bcache :batch-list)]
      (schedule-job (key job) (val job))))
  (stop [_]
    ; Any stuff to stop
    ))