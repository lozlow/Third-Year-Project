(ns dataproc.site-specific.analysis.core
  (:require [datomic.api :as d]
            [dataproc.db.datomic :as dbd]))

(defn endpoint
  [data]
  (let [db (dbd/get-db)]
    (println (apply str (take 5 (d/q '[:find ?title
                                      :in $ ?a
                                      :where
                                      [?t :track/artists ?a]
                                      [?t :track/name ?title]] db data))))))
