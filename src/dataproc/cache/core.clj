(ns dataproc.cache.core
  (:require [dataproc.config :as config]
            [immutant.cache :as cache]
            [taoensso.timbre :as log])
  (:use     [dataproc.util]))

(defn create-cache
  [name]
  (cache/lookup-or-create name
                          :tx true
                          :locking :pessimistic
                          :persist (app-path "/cache")))

(defn get-cache
  [name]
  (cache/lookup name))

(defn init
  []
  (log/info "Creating cache: dbscanner")
  (create-cache "dbscanner"))