(ns dataproc.services.batchrunner
  (:require [immutant.daemons :as daemon]
            [dataproc.config :as config]
            [dataproc.cache.core :as cache]
            [immutant.cache :as dcache]
            [immutant.jobs :as q]))

(declare ^:private bcache)

(defn register-batch
  [name fn]
  ; Note for report - this is so that if the daemon went down, taking the query with it,
  ; they would be in the cache and started when the daemon is started on another node
  (dcache/swap! bcache :batch-list conj (hash-map name fn))
  (println "BATCH-LIST" (get bcache :batch-list)))

  ; This is for removing from batch-list
  ; (cache/swap! bcache :batch-list dissoc (get bcache :batch-list) name)
  
  ; start-job
  ; add name to :running-batch-list
  ; when it ends remove from :running-batch-list
  
(defn init
  []
  (def bcache (cache/create-cache "batchrunner"))
  (dcache/put bcache :batch-list {}))

(defrecord BatchRunner []
  daemon/Daemon
  (start [_]
    (dcache/put bcache :running-batch-list {})
    ; Schedule batches from cache
    )
  (stop [_]
    ; Any stuff to stop
    ))