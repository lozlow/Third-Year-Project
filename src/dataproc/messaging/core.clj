(ns dataproc.messaging.core
  (:require [dataproc.config :as config]
            [datomic.api :as d]
            [immutant.messaging :as msg]
            [taoensso.timbre :as log]))

(defn register-listener
  [qt endpoint concurrency]
  (msg/listen qt endpoint :concurrency concurrency))

(defn start-message-service
  [qt]
  (msg/start qt :durable true :address-full-message-policy :block))

(defn- register-message-service-from-map
  [map concurrency]
  (dorun (map (fn [[key val]]
                (log/info (str "Starting " key " message service"))
                (start-message-service key)
                (register-listener key (unquote val) concurrency)) map)))
  

(defn init
  "Initialisation function"
  []
  (log/info "Initialising messaging services")
  (register-message-service-from-map (config/get-config :work-queue) (config/get-config :num-work-threads))
  (register-message-service-from-map (config/get-config :adhoc-queue) (config/get-config :num-adhoc-threads)))
