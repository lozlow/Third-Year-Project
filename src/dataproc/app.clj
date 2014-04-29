(ns dataproc.app
  (:require [immutant.daemons :as daemon]
            [dataproc.services.dbscanner :as dbscanner]
            [dataproc.services.batchrunner :as batchrunner]
            [dataproc.config :as config]
            [dataproc.messaging.core :as messaging]
            [dataproc.cache.core :as cache]
            [taoensso.timbre :as timbre]
            [taoensso.timbre.appenders.rotor :as rotor]
            [dataproc.db.postgres :as pdb]
            [dataproc.db.datomic :as ddb])
  (:use     [dataproc.util])
  (:import  [dataproc.services.dbscanner DBScanner]
            [dataproc.services.batchrunner BatchRunner]))

(defn init
  []
  (config/load-config (app-path "/config/app.config.edn"))
  
  (timbre/set-config!
    [:appenders :rotor]
    {:min-level :info
     :enabled? true
     :async? false ; should be always false for rotor
     :max-message-per-msecs nil
     :fn rotor/appender-fn})
  
  (timbre/set-config!
    [:shared-appender-config :rotor]
    {:path (app-path "/log/dataproc.log") :max-size (* 512 1024) :backlog 10})
  
  (ddb/init)
  (pdb/init)
  
  (cache/init)
  (messaging/init)
  
  (dbscanner/init)
  (batchrunner/init)
  
  (daemon/create "dbscanner" (DBScanner.) :singleton true)
  (daemon/create "batchrunner" (BatchRunner.) :singleton true))
