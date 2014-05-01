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
(defn wrap-job
  "name - the name of the job
   fnc  - the function to be executed

   Returns a function that will be called by the Quartz scheduler.
   If running-batch-list contains the job name, it is currently
   being executed and so will do nothing. Otherwise it adds the job
   to the running list, executes the function then removes the job from
   the running list"
  [name fnc]
  (letfn [(func [name fnc] 
                (fn []
                  (when-not (contains? (get bcache :running-batch-list) name)
				                (do
				                  (add-job-to-running-list name)
				                  (fnc)
				                  (remove-job-from-running-list name)))))]
    (intern *ns* (symbol (str "_" name)) (func (keyword name) fnc))))

(defn schedule-job
  "This could be written much nicer"
  ([name fnc]
    (let [func (require-fn fnc)]
		  (log/info "Scheduling batch query job:" name "to run" func "every 5 minutes")
		  (q/schedule name (wrap-job name func) :every [5 :minutes])))
  ([name fnc & args]
    (let [func (require-fn fnc)]
		  (log/info "Scheduling batch query job:" name "to run" func "with args" args) ; Change
		  (apply (partial q/schedule name (wrap-job name func)) args))))

(defn register-batch
  [name fnc & args]
  ; Note for report - this is so that if the daemon went down, taking the query with it,
  ; they would be in the cache and started when the daemon is started on another node
  (dcache/swap! bcache :batch-list assoc name fnc)
  (println "BATCH-LIST" (get bcache :batch-list))
  (apply (partial schedule-job name fnc) args))

(defn unregister-batch
  [name]
  ; This is for removing from batch-list
  (dcache/swap! bcache :batch-list dissoc (get bcache :batch-list) name)
  (println "BATCH-LIST" (get bcache :batch-list))
  (q/unschedule name))
  
(defn init
  []
  (def bcache (cache/create-cache "batchrunner" :persist nil))
  (dcache/put-if-absent bcache :batch-list {}))

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