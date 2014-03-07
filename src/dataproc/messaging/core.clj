(ns dataproc.messaging.core
  (:require [dataproc.config :as config]
            [datomic.api :as d]
            [immutant.messaging :as msg]
            [immutant.messaging.hornetq :as hornetq]
            [taoensso.timbre :as log]
            [clojure.string :as str]
            [dataproc.util :as util :refer [require-fn]]))

(defn register-listener
  [qt endpoint concurrency]
  (msg/listen qt endpoint :concurrency concurrency))

(defn start-message-service
  [qt]
  (msg/start qt :durable false)) ; Temporary - should be true in production

(defn- register-message-service-from-vec
  "TODO Split this into separate functions"
  [cfgops]
  (loop [[{qt :msgserv {:keys [endpoint concurrency]} :params} :as vec] cfgops]
    (when-not (empty? vec)
      (log/info "Starting" qt "messaging service")
      (start-message-service qt)
      (log/info "Registering listener for" qt ", calling function" endpoint)
      (register-listener qt (require-fn endpoint) concurrency)
      (recur (rest vec)))))
  

(defn init
  "Initialisation function"
  []
  (log/info "Initialising messaging services")
  (register-message-service-from-vec (config/get-config :work-queue))
  (register-message-service-from-vec (config/get-config :adhoc-queue)))
