(ns dataproc.services.dbscanner
  (:require [immutant.daemons :as daemon]
            [dataproc.config :as config]
            [datomic.api :as d]
            [dataproc.db.datomic :as dbd]
            [immutant.messaging :as msg]
            [taoensso.timbre :as log]
            [immutant.cache :as cache]))

(def ^:private done (atom false))

(def ^:private dcache (cache/lookup-or-create "dataproc" :persist "/app/dataproc/cache"))

(defn endpoint
  [data]
  (let [conn (d/connect (config/get-config :datomic-uri))
        db (d/db conn)]
    (println (apply str (take 5 (d/q '[:find ?title
                                      :in $ ?a
                                      :where
                                      [?t :track/artists ?a]
                                      [?t :track/name ?title]] db data))))))


(defrecord DBScanner []
  daemon/Daemon
  (start [_]
    (reset! done false)
    (log/info "Initialising cache")
    (loop [i 0]
        (let [db (dbd/get-db)]
          (msg/with-connection {}
            (loop [result (d/seek-datoms db :aevt :artist/name (get dcache :last-ref))]
              (when-not @done
                (log/debug (str "Publishing to work queue: " (:e (first result))))
                (msg/publish "/queue/dataproc/work/" (:e (first result)))
                (cache/put dcache :last-ref (:e (first result)))
                (Thread/sleep 1000)
                (recur (rest result))))))
        (recur (inc i))))
  (stop [_] (reset! done true)))