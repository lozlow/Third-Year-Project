(ns dataproc.site-specific.db.core
  (:require [dataproc.db.datomic :as ddb]
            [datomic.api :as d]))

(defn tracks-for-country
  [country]
  (d/q '[:find ?title
         :in $ ?country
         :where
         [?c :country/name ?country]
         [?a :artist/country ?c]
         [?t :track/artists ?a]
         [?t :track/name ?title]]
       (ddb/get-db)
       country))

(defn list-countries
  []
  (d/q '[:find ?country
         :in $
         :where
         [?c :country/name ?country]]
       (ddb/get-db)))