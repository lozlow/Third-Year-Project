(ns dataproc.site-specific.core
  (:require [dataproc.config :as config]
            [dataproc.services.batchrunner :as batchrunner]
            [taoensso.timbre :as log]))

(defn configure-batches
  [batch-list]
  (doseq [{:keys [name fnargs schedule]} batch-list]
    (apply (partial batchrunner/register-batch name fnargs) schedule)))

(defn init
  []
  (log/info "Loading batches from configuration file")
  (configure-batches (config/get-config :batches)))