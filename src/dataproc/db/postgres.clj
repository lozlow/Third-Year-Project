(ns dataproc.db.postgres
  (:require [dataproc.config :as config]
            [clojure.java.jdbc :as jdbc]
            [taoensso.timbre :as log])
  (:use     [immutant.util :only [at-exit]]
            [clojure.string :only [join]])
  (:import  [com.mchange.v2.c3p0 ComboPooledDataSource]))

(def ^:private pool-ref nil)

(defn ^:private pool
  "Reference http://clojure-doc.org/articles/ecosystem/java_jdbc/connection_pooling.html

   Also, note for report on why pool is necessary"
  [spec]
  (let [cpds (doto (ComboPooledDataSource.)
               (.setDriverClass (:classname spec)) 
               (.setJdbcUrl (str "jdbc:" (:subprotocol spec) ":" (:subname spec)))
               (.setUser (:user spec))
               (.setPassword (:password spec))
               ;; expire excess connections after 30 minutes of inactivity:
               (.setMaxIdleTimeExcessConnections (* 30 60))
               ;; expire connections after 3 hours of inactivity:
               (.setMaxIdleTime (* 3 60 60)))]
    (def pool-ref cpds)
    {:datasource cpds}))

(def ^:private pooled-db (delay (pool (config/get-config :results-store-uri))))

(defn db-connection [] @pooled-db)

(defn gen-create-table-string ; DOESN'T WORK YET
  [tablename schema]
  (str "CREATE TABLE IF NOT EXISTS "
       tablename
	     " ("
	     (join "," (map name schema))
	     ")"))

(defn create-table
  [tablename schema]
  (jdbc/db-do-commands (db-connection) tablename))

(defn enter-result
  [result]
  (jdbc/insert! (db-connection) :dproc_result result))

(defn shutdown
  []
  (log/info "Gracefully shutting down c3p0 database connection pool")
  (.close pool-ref))

(defn init
  []
  (log/info "Initialising PostgreSQL database tables")
  (create-table "CREATE TABLE IF NOT EXISTS dproc_result (ent_id bigint, matched_rules text, info text)" '["ent_id" :bigint "result" :text])
  (create-table "CREATE TABLE IF NOT EXISTS dproc_batches (batch_name text, result text)" '["ent_id" :bigint "result" :text]) ; THIS SHOULD BE MOVED TO SITE_SPECIFIC THINGY
  (at-exit shutdown))
    