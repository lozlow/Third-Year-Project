(ns dataproc.site-specific.analysis.core
  (:require [datomic.api :as d]
            [dataproc.db.datomic :as ddb]
            [dataproc.db.postgres :as pdb]
            [taoensso.timbre :as log]))

(defn is-collab?
  [^String str]
  (or (.contains (.toLowerCase str) "feat")))

(defn endpoint
  [data]
  (let [db (ddb/get-db)
        analysis (for [[e artist song] (vec (d/q '[:find ?e ?artist ?title
                                              :in $ ?e
                                              :where
                                              [?t :track/artists ?e]
                                              [?e :artist/name ?artist]
                                              [?t :track/name ?title]] db data))
                       :when (is-collab? song)]
                   {:artist artist :song song})]
    (when-not (empty? analysis)
      (pdb/enter-result data analysis))))