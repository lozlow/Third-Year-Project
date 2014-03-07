(ns dataproc.services.dbscanner
  (:require [immutant.daemons :as daemon]
            [dataproc.config :as config]
            [datomic.api :as d]
            [dataproc.db.datomic :as dbd]
            [immutant.messaging :as msg]
            [taoensso.timbre :as log]
            [immutant.cache :as cache]
            [immutant.messaging.hornetq :as hornetq])
  (:import  [org.hornetq.api.jms.management JMSQueueControl]))

(def ^:private done (atom false))

(def ^:private dcache (cache/lookup-or-create "dataproc" :persist "/app/dataproc/cache"))

(defrecord DBScanner []
  daemon/Daemon
  (start [_]
    (reset! done false)
    (log/info "Initialising cache")
    (loop [i 0]
        (let [db (dbd/get-db)
              qc (hornetq/destination-controller "/queue/dataproc/work/")]
          (msg/with-connection {}
            (doseq [result (map :e (take 2000 (d/seek-datoms db :aevt :artist/name (get dcache :last-ref))))]
                ;(log/debug (str "Publishing to work queue: " result))
                (msg/publish "/queue/dataproc/work/" result)
                (cache/swap! dcache :last-ref result)
                (println (str "Queue length:" (.countMessages qc nil))))))
        (Thread/sleep 120000)
        (recur (inc i))))
  (stop [_] (reset! done true)))
