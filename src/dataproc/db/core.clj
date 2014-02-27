(ns dataproc.db.core
  (require [dataproc.config :as config]
           [datomic.api :as d]))

(def ^:private conn (d/connect (config/get-config :datomic-uri)))

(def ^:private db (d/db conn))
