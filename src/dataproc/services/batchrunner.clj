(ns dataproc.services.batchrunner
  (:require [immutant.daemons :as daemon]
            [dataproc.config :as config]
            [dataproc.cache.core :as cache]
            [immutant.cache :as dcache]
            [taoensso.timbre :as log]
            [immutant.jobs :as q]
            [clojure.string :as string])
  (:use     [dataproc.util :only (require-fn)]))

(def ^:private running (atom false))

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
  [name fnargs]
  (letfn [(func [name [fnc :as fnargs]] 
                (fn []
	                  (if (contains? (get bcache :running-batch-list) name)
	                    (log/info "Batch" name "already running, aborting")
                      (let [func (require-fn fnc)]
	                        (do
	                          (log/info "Starting batch" name)
			                      (add-job-to-running-list name)
			                      (apply func (rest fnargs))
			                      (remove-job-from-running-list name))))))]
    (intern *ns* (symbol (str "_" name)) (func (keyword name) fnargs))))

(defn schedule-job
  "This could be written much nicer"
  ([name [fnc :as fnargs]]
    (log/info "Scheduling batch query job:" name "to run" fnargs "every 5 minutes")
		(q/schedule name (wrap-job name fnargs) :every [5 :minutes]))
  ([name [fnc :as fnargs] & args]
    (log/info "Scheduling batch query job:" name "to run" fnargs "with args" args) ; Change
		(apply (partial q/schedule name (wrap-job name fnargs)) args)))

(defn register-batch
  [name fnargs & args]
  ; Note for report - this is so that if the daemon went down, taking the query with it,
  ; they would be in the cache and started when the daemon is started on another node
  (let [name (string/replace name #"\s" "-")]
	  (dcache/swap! bcache :batch-list assoc name fnargs)
	  (println "BATCH-LIST" (get bcache :batch-list))
	  (apply (partial schedule-job name fnargs) args)))

(defn unregister-batch
  [name]
  ; This is for removing from batch-list
  (dcache/swap! bcache :batch-list dissoc (get bcache :batch-list) name)
  (println "BATCH-LIST" (get bcache :batch-list))
  (q/unschedule name))

(defn ^:private stats-fn
  []
  (while (true? @running)
    (log/report {:batch-list (get bcache :batch-list)
                 :running-batch-list (get bcache :running-batch-list)})
    (Thread/sleep 10000)))
  
(defn init
  []
  (def bcache (cache/create-cache "batchrunner" :persist nil))
  (dcache/put-if-absent bcache :batch-list {}))

(defrecord BatchRunner []
  daemon/Daemon
  (start [_]
    (reset! running true)
    (log/info "Starting BatchRunner service")
    (dcache/put bcache :running-batch-list #{})
    
    ; Schedule batches from cache
    (doseq [job (get bcache :batch-list)]
      (schedule-job (key job) (val job)))
    
    (.start (Thread. stats-fn)))
  (stop [_]
    (reset! running false)
    (log/info "Stopping BatchRunner service")))