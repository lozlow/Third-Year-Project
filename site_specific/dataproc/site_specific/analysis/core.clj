(ns dataproc.site-specific.analysis.core
  (:require [datomic.api :as d]
            [dataproc.db.datomic :as dbd]))

(defn is-collab?
  [^String str]
  (or (.contains str "feat")))

(defn endpoint
  [data]
  (let [db (dbd/get-db)
        analysis (for [[e artist song] (vec (d/q '[:find ?e ?artist ?title
                                              :in $ ?e
                                              :where
                                              [?t :track/artists ?e]
                                              [?e :artist/name ?artist]
                                              [?t :track/name ?title]] db data))
                       :when (is-collab? song)]
                   {:e e :artist artist :song song})]
    (when-not (empty? analysis)
      (println analysis))))