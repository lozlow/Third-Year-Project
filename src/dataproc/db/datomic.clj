(ns dataproc.db.datomic
  (:require [dataproc.config :as config]
            [datomic.api :as d]
            [immutant.messaging :as msg]
            [taoensso.timbre :as log]))

(declare ^:private db)

(defn connect-db
  "Connects to the database and returns a database object"
  [uri]
  (let [conn (d/connect uri)]
    (def db (d/db conn))
    db))

(defn get-db
  "Returns the database, connects if the database is not connected
   
   As such, this is an impure function."
  []
  (if (nil? db)
    (connect-db)
    db))

(defn init
  []
  (log/info "Attempting to connect to Datomic database")
  (connect-db (config/get-config :datomic-uri)))