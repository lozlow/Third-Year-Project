(ns dataproc.messaging.core
  (:require [dataproc.config :as config]
            [datomic.api :as d]
            [immutant.messaging :as msg]
            [immutant.messaging.hornetq :as hornetq]
            [taoensso.timbre :as log]
            [clojure.string :as str])
  (:use     [dataproc.util :only [require-fn]]))

(defn register-listener
  [qt endpoint concurrency]
  (msg/listen qt endpoint :concurrency concurrency))

(defn start-message-service
  [qt]
  (msg/start qt :durable false)) ; Temporary - should be true in production

(defn- register-message-service-from-map
  "TODO Split this into separate functions"
  [cfgops]
  (let [{qt :msgserv {:keys [endpoint concurrency]} :params} cfgops]
    (log/info "Starting" qt "messaging service")
    (start-message-service qt)
    (log/info "Registering listener for" qt ", calling function" endpoint)
    (register-listener qt (require-fn endpoint) concurrency)))
  

(defn init
  "Initialisation function"
  []
  (log/info "Initialising messaging services")
  (register-message-service-from-map (config/get-config :work-queue))
  (register-message-service-from-map (config/get-config :adhoc-queue)))
