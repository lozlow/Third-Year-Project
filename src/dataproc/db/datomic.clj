(ns dataproc.db.datomic
  (require [dataproc.config :as config]
           [datomic.api :as d]
           [immutant.messaging :as msg]
           [taoensso.timbre :as log]))

(declare ^:private conn)
(declare ^:private db)

; Temporary
(defn init
  []
  (log/info "Attempting to connect to Datomic database")
  (def conn (d/connect (config/get-config :datomic-uri)))
  (def db (d/db conn)))

(defn get-db
  "Returns the database"
  []
  db)