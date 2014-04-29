(ns dataproc.services.batchrunner
  (:require [immutant.daemons :as daemon]
            [dataproc.config :as config]
            [dataproc.cache.core :as cache]
            [immutant.cache :as dcache]
            [taoensso.timbre :as log]
            [immutant.jobs :as q])
  (:use     [dataproc.util :only (require-fn)]))

(declare ^:private bcache)

(defn hello
  []
  (log/info "Hello from batch!"))

(defn- add-job-to-running-list
  [name]
  (dcache/swap! bcache :running-batch-list conj name))

(defn- remove-job-from-running-list
  [name]
  (dcache/swap! bcache :running-batch-list disj name))

  ; start-job
  ; add name to :running-batch-list
  ; when it ends remove from :running-batch-list
(defmacro wrap-job
  "name - the name of the job
   fnc  - the function to be executed

   Returns a function that will be called by the Quartz scheduler.
   If running-batch-list contains the job name, it is currently
   being executed and so will do nothing. Otherwise is adds the job
   to the running list, execute the function then remove the job from
   the running list"
  [name fnc]
  (let [name (keyword name)]
	  `(fn []
	     (when-not (contains? (get bcache :running-batch-list) ~name)
	       (do
	         (add-job-to-running-list ~name)
	         (~fnc)
	         (remove-job-from-running-list ~name))))))

(defn schedule-job
  "This could be written much nicer"
  ([name fnc]
	  (log/info "Scheduling batch query job:" name "to run every 5 minutes")
	  (q/schedule name (wrap-job name fnc) :every [5 :minutes]))
  ([name fnc & args]
	  (log/info "Scheduling batch query job:" name "to run with args" args) ; Change
	  (apply (partial q/schedule name (wrap-job name fnc)) args)))

(defn register-batch
  [name fnc & args]
  ; Note for report - this is so that if the daemon went down, taking the query with it,
  ; they would be in the cache and started when the daemon is started on another node
  (let [func (symbol fnc)]
    (require-fn fnc)
	  (dcache/swap! bcache :batch-list assoc name func)
	  (println "BATCH-LIST" (get bcache :batch-list))
	  (apply (partial schedule-job name func) args)))

(defn unregister-batch
  [name]
  ; This is for removing from batch-list
  (dcache/swap! bcache :batch-list dissoc (get bcache :batch-list) name)
  (println "BATCH-LIST" (get bcache :batch-list))
  (q/unschedule name))
  
(defn init
  []
  (def bcache (cache/create-cache "batchrunner"))
  (dcache/put bcache :batch-list {}))

(defrecord BatchRunner []
  daemon/Daemon
  (start [_]
    (log/info "Starting BatchRunner service")
    (dcache/put bcache :running-batch-list #{})
    ; Schedule batches from cache
    (doseq [job (get bcache :batch-list)]
      (schedule-job (key job) (val job))))
  (stop [_]
    ; Any stuff to stop
    (log/info "Stopping BatchRunner service")))