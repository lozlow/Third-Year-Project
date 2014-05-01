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
      (= songrelease (- labelend 1)))))

(defn handle-match
  [val & args]
  (log/info "MATCHED!" [val (vec args)]))

(defn endpoint
  [data]
  (a/ruleset
    data
    handle-match
    (a/rule "song release on same year as label shut down" release-on-same-year-as-label-shutdown)
    (a/rule "song release on previous year as label shut down" release-on-prev-year-to-label-shutdown)))
