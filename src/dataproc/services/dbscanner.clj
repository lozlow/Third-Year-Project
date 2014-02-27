(ns dataproc.services.dbscanner
  (:require [immutant.daemons :as daemon]
            [dataproc.config :as config]))

(def ^:private done (atom false))

(defrecord DBScanner []
  daemon/Daemon
  (start [_]
    (reset! done false)
    (loop [i 0]
      (Thread/sleep 1000)
      (when-not @done
        (println (config/get-config))
        (recur (inc i)))))
  (stop [_] (reset! done true)))