(ns dataproc.app
  (:require [immutant.daemons :as daemon]
            [dataproc.services.dbscanner :as dbscanner]
            [dataproc.config :as config]
            [dataproc.messaging.core :as messaging]
            [taoensso.timbre :as timbre]
            [taoensso.timbre.appenders.rotor :as rotor]
            [dataproc.db.postgres :as pdb]
            [dataproc.db.datomic :as ddb])
  (:import  [dataproc.services.dbscanner DBScanner]))

(defn init
  []
  (config/load-config "/app/dataproc/config/app.config.edn")
  
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
  
  (ddb/init)
  (pdb/init)
  
  (messaging/init)
  
  (daemon/create "dbscanner" (DBScanner.) :singleton true))
