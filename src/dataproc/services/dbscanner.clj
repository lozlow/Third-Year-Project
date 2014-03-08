(ns dataproc.services.dbscanner
  (:require [immutant.daemons :as daemon]
            [dataproc.config :as config]
            [datomic.api :as d]
            [dataproc.db.datomic :as dbd]
            [immutant.messaging :as msg]
            [taoensso.timbre :as log]
            [immutant.cache :as cache]))

(def ^:private done (atom false))

(def ^:private dcache (cache/lookup-or-create "dataproc" :tx false :persist "/app/dataproc/cache"))

(defrecord DBScanner []
  daemon/Daemon
  (start [_]
    (reset! done false)
    (log/info "Initialising cache")
    (loop [i 0]
        (let [db (dbd/get-db)]
          (msg/with-connection {}
            (doseq [result (map :e (take 20000 (d/seek-datoms db :aevt :artist/name (get dcache :last-ref))))]
                (log/debug (str "Publishing to work queue: " result))
                (msg/publish "/queue/dataproc/work/" result)
                (cache/put dcache :last-ref result))))
        (Thread/sleep 120000)
        (recur (inc i))))
  (stop [_] (reset! done true)))
