(ns dataproc.db.datomic
  (:require [dataproc.config :as config]
            [datomic.api :as d]
            [immutant.messaging :as msg]
            [taoensso.timbre :as log]))

(declare ^:private db)

(defn index-datoms
  "Returns the index for a given attribute, optionally starting from a
   given entity id"
  ([attribute]
    (d/seek-datoms db :aevt attribute))
  ([attribute start-eid]
    (d/seek-datoms db :aevt attribute start-eid)))

(defn connect-db
  "Connects to the database and returns a database object"
  [uri]
  (let [conn (d/connect uri)]
    (def db (d/db conn))
    db))

(defn get-db
  "Returns the database"
  []
  db)

(defn init
  []
  (log/info "Attempting to connect to Datomic database")
  (connect-db (config/get-config :datomic-uri)))