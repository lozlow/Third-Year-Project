(ns dataproc.services.dbscanner
  (:require [immutant.daemons :as daemon]
            [dataproc.config :as config]
            [datomic.api :as d]
            [dataproc.db.datomic :as ddb]
            [immutant.messaging :as msg]
            [taoensso.timbre :as log]
            [immutant.cache :as cache])
  (:import  [java.util.concurrent Executors]))

(def ^:private tpool (Executors/newFixedThreadPool 4))

(def ^:private dcache (cache/lookup-or-create "dataproc" :tx false :persist "/app/dataproc/cache"))

(defn workFn
  [params]
  (let [{:keys [last-ref end-ref]} params
       entids (map :e (ddb/index-datoms :artist/name last-ref))]
   (msg/with-connection {}
     (doseq [entid (take-while (partial not= end-ref) entids)]
         (log/debug (str "Publishing to work queue: " entid))
         (msg/publish "/queue/dataproc/work/" entid)
         (cache/put dcache :last-ref entid)))))

(defn- generate-work-params
  [start-ref]
  (let [entids (map :e (take 10 (ddb/index-datoms :artist/name start-ref)))
       next (last entids)]
    (cache/put dcache :next-ref next)
    {:start-ref start-ref
     :end-ref next
     :last-ref start-ref}))
    
(defn- start-workfn
  []
  (workFn (generate-work-params (get dcache :next-ref))))

(defrecord DBScanner []
  daemon/Daemon
  (start [_]
    (log/info "Starting DBScanner")
    (.submit tpool start-workfn))
  (stop [_]
    (.shutdown tpool)))
