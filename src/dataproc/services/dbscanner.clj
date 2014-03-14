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
  (let [db (dbd/get-db)
       entids (map :e (take 50000 (d/seek-datoms db :aevt :artist/name (get dcache :last-ref))))
       result (butlast entids)
       lastid (last result)
       next (last entids)]
   (msg/with-connection {}
     (doseq [entid entids]
         (log/debug (str "Publishing to work queue: " entid))
         (msg/publish "/queue/dataproc/work/" entid)
         (cache/put dcache :last-ref entid)))))

(defrecord DBScanner []
  daemon/Daemon
  (start [_]
    (log/info "Starting DBScanner")
    (.submit tpool workFn))
  (stop [_]
    (.shutdown tpool)))
