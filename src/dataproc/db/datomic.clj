(ns dataproc.db.datomic
  (:require [dataproc.config :as config]
            [datomic.api :as d]
            [immutant.messaging :as msg]
            [taoensso.timbre :as log]))

(declare ^:private db)
(declare ^:private conn)

(defn connect-db
  "Connects to the database and returns a database object"
  [uri]
  (let [conn (d/connect uri)]
    (def db (d/db conn))))

(defn get-db
  "Returns the database, connects if the database is not connected
   
   As such, this is an impure function."
  []
  db)

(defn init
  []
  (log/info "Attempting to connect to Datomic database")
  (def conn (d/connect (config/get-config :datomic-uri)))
  (def db (d/db conn)))
  ;(connect-db (config/get-config :datomic-uri)))