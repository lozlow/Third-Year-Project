(ns dataproc.services.dbscanner
  (:require [immutant.daemons :as daemon]
            [dataproc.config :as config]
            [datomic.api :as d]
            [immutant.messaging :as msg]
            [taoensso.timbre :as log]))

(def ^:private done (atom false))

(def ^:private counter (atom 1))

(defn endpoint
  [data]
  (println data)
  (reset! counter (inc @counter)))

(defrecord DBScanner []
  daemon/Daemon
  (start [_]
    (reset! done false)
    (msg/start "/queue/dataproc/work/")
    (log/info "Starting dataproc/work message queue")
    (msg/listen "/queue/dataproc/work/" endpoint :concurrency 10)
    (log/info "Registering listeners on /dataproc/work/ message queue")
    (loop [i 0]
      (when-not @done
        (let [conn (d/connect (config/get-config :datomic-uri))
             db (d/db conn)]
          (log/info "Querying database...")
          (println "Hello 1")
          (msg/with-connection {}
            (loop [result (map :e (take 100 (d/seek-datoms db :aevt :artist/name)))]
              (when-not (empty? result)
                (do
                  (msg/publish "/queue/dataproc/work/" (first result))
                  (recur (rest result))))))
          (println (str "Hello 2: " @counter)))
        (Thread/sleep 120000)
        (recur (inc i)))))
  (stop [_] (reset! done true)))