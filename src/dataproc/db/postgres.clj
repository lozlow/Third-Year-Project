(ns dataproc.db.postgres
  (:require [clojure.java.jdbc :as jdbc]
            [taoensso.timbre :as log])
  (:import  [com.mchange.v2.c3p0 ComboPooledDataSource]))

(def db-spec {:classname "org.postgresql.Driver"
              :subprotocol "postgresql"              :subname "//localhost:5432/datomic"
              :user "datomic"
              :password "datomic"})

(defn ^:private pool
  "Reference http://clojure-doc.org/articles/ecosystem/java_jdbc/connection_pooling.html

   Also, note for report"
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
    {:datasource cpds}))

(def ^:private pooled-db (delay (pool db-spec)))

(defn db-connection [] @pooled-db)

(defn create-results-table
  []
  (jdbc/db-do-commands (db-connection) "CREATE TABLE IF NOT EXISTS dproc_result (ent_id bigint, result text)"))

(defn init
  []
  (log/info "Initialising PostgreSQL database tables")
  ; (create-results-table)) <== Temporarily disable this until migrated to 9.1 on debian
  )
    