(ns dataproc.services.dbscanner
  (:require [immutant.daemons :as daemon]
            [dataproc.config :as config]
            [datomic.api :as d]
            [dataproc.db.datomic :as dbd]
            [immutant.messaging :as msg]
            [taoensso.timbre :as log]
            [immutant.cache :as cache])
  (:import  [java.util.concurrent Executors]))

(def ^:private tpool (Executors/newFixedThreadPool 4))

(def ^:private dcache (cache/lookup-or-create "dataproc" :tx false :persist "/app/dataproc/cache"))

(defn workFn
  []
  (let [db (dbd/get-db)]
          (msg/with-connection {}
            (doseq [result (map :e (d/seek-datoms db :aevt :artist/name (get dcache :last-ref)))]
                (log/debug (str "Publishing to work queue: " result))
                (msg/publish "/queue/dataproc/work/" result)
                (cache/put dcache :last-ref result)))))

(defrecord DBScanner []
  daemon/Daemon
  (start [_]
    (.submit tpool workFn))
  (stop [_]
    (.shutdown tpool)))
