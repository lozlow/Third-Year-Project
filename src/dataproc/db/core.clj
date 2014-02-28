(ns dataproc.db.core
  (require [dataproc.config :as config]
           [datomic.api :as d]
           [immutant.messaging :as msg]
           [taoensso.timbre :as log]))

(declare ^:private conn)
(declare ^:private db)

; Temporary
(defn init
  []
  (def conn (d/connect (config/get-config :datomic-uri)))
  (def db (d/db conn)))