(ns dataproc.app
  (:require [immutant.daemons :as daemon]
            [dataproc.services.dbscanner :as dbscanner]
            [dataproc.config :as config]
            [taoensso.timbre :as timbre]
            [taoensso.timbre.appenders.rotor :as rotor]
            [dataproc.db.core :as db])
  (:import  [dataproc.services.dbscanner DBScanner]))

(defn init
  []
  (daemon/create "dbscanner" (DBScanner.) :singleton true)
  (config/load-config "/app/dataproc/resources/app.config.edn")
  
  (timbre/set-config!
    [:appenders :rotor]
    {:min-level :info
     :enabled? true
     :async? false ; should be always false for rotor
     :max-message-per-msecs nil
     :fn rotor/appender-fn})
  
  (timbre/set-config!
    [:shared-appender-config :rotor]
    {:path "dataproc.log" :max-size (* 512 1024) :backlog 10})
  
  (db/init))