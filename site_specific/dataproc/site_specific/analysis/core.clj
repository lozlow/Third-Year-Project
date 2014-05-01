(ns dataproc.site-specific.analysis.core
  (:require [datomic.api :as d]
            [dataproc.db.datomic :as ddb]
            [dataproc.db.postgres :as pdb]
            [taoensso.timbre :as log]
            [dataproc.analysis.core :as a]))

(defn release-on-same-year-as-label-shutdown
  [entid]
  (let [db (ddb/get-db)
        [_ songrelease labelend :as res] (flatten (vec (d/q '[:find ?e ?songreleaseyear ?labelendyear
		                                                          :in $ ?e
		                                                          :where
		                                                          [?m :medium/tracks ?e]
		                                                          [?r :release/media ?m]
		                                                          [?r :release/year ?songreleaseyear]
		                                                          [?r :release/label ?l]
		                                                          [?l :label/endYear ?labelendyear]] db entid)))]
    (if (empty? res)
      nil
      (= songrelease labelend))))

(defn release-on-prev-year-to-label-shutdown
  [entid]
  (let [db (ddb/get-db)
        [songrelease labelend :as res] (flatten (vec (d/q '[:find ?songreleaseyear ?labelendyear
		                                                :in $ ?e
		                                                :where
		                                                [?m :medium/tracks ?e]
		                                                [?r :release/media ?m]
		                                                [?r :release/year ?songreleaseyear]
		                                                [?r :release/label ?l]
		                                                [?l :label/endYear ?labelendyear]] db entid)))]
    (if (empty? res)
      nil
      (= songrelease (- labelend 1)))))

(defn handle-match
  [entid & args]
  (let [db (ddb/get-db)
        info (flatten (vec (d/q '[:find ?artist ?song
		                                   :in $ ?e
		                                   :where
		                                   [?e :track/name ?song]
                                       [?e :track/artists ?a]
                                       [?a :artist/name ?artist]] db entid)))]
    (pdb/enter-result {:ent_id entid
                       :matched_rules (pr-str (vec args))
                       :info (pr-str (zipmap '(:artist :title) info))})))

(defn endpoint
  [data]
  (a/ruleset
    data
    handle-match
    (a/rule "song release on same year as label shut down" release-on-same-year-as-label-shutdown)
    (a/rule "song release on previous year as label shut down" release-on-prev-year-to-label-shutdown)))
