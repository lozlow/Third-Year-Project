(ns dataproc.cache.core
  (:require [dataproc.config :as config]
            [immutant.cache :as cache]
            [taoensso.timbre :as log])
  (:use     [dataproc.util]))

(defn create-cache
  "Wraps around the Immutant create cache function, creating a cache
   if one does not exist, with some project specific options"
  [name]
  (cache/lookup-or-create name
                          :tx true
                          :locking :pessimistic
                          :persist (app-path "/cache")))

(defn get-cache
  "Simple wrapper around Immutant's lookup function"
  [name]
  (cache/lookup name))

(defn init
  "Initialise function

   Sets up project caches"
  []
  (log/info "Creating cache: dbscanner")
  (create-cache "dbscanner"))