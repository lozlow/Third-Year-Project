(ns dataproc.app
  (:require [immutant.daemons :as daemon]
            [dataproc.services.dbscanner :as dbscanner]
            [dataproc.config :as config])
  (:import  [dataproc.services.dbscanner DBScanner]))

(defn init
  []
  (daemon/create "dbscanner" (DBScanner.) :singleton true)
  (config/load-config "/app/dataproc/resources/app.config.edn")
  (println (config/get-config)))

