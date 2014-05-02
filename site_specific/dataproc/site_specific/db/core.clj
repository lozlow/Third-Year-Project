(ns dataproc.site-specific.db.core
  (:require [dataproc.db.datomic :as ddb]
            [dataproc.db.postgres :as pdb]
            [clojure.java.jdbc :as jdbc]
            [taoensso.timbre :as log] ;TMP
            [datomic.api :as d])
  (:use     [dataproc.util :only [require-fn]]))

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

(defn insert-or-update-batch
  [batchname result]
  (let [db (pdb/db-connection)
        result (pr-str result)]
    (if (not (empty? (jdbc/query
                       db
                       [(str "select 1 from dproc_batches where batch_name = '" batchname "'")])))
      (jdbc/update! db :dproc_batches {:result result} ["batch_name = ?" batchname])
      (jdbc/insert! db :dproc_batches {:batch_name batchname :result result}))))

(defn do-batch
  [batchname batchfn & args]
  (let [batchfn (require-fn batchfn)
        result (apply batchfn args)]
    (insert-or-update-batch batchname result)
    (log/info "Writing batch to DB")))

(defn create-tables
  []
  (pdb/create-table "dproc_batches" '["batch_name" :text "result" :text]))

(defn init
  []
  (create-tables)) ; This will be automatically called by site-specific init function