(ns dataproc.cache.core
  (:require [dataproc.config :as config]
            [immutant.cache :as cache]
            [taoensso.timbre :as log]))

(defn create-cache
  [name]
  (cache/lookup-or-create name :tx false :persist "/app/dataproc/cache"))

(defn get-cache
  [name]
  (cache/lookup name))

(defn init
  []
  (log/info "Creating cache: dbscanner")
  (create-cache "dbscanner"))