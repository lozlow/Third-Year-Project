(ns dataproc.services.dbscanner
  (:require [immutant.daemons :as daemon]
            [dataproc.config :as config]
            [datomic.api :as d]
            [dataproc.db.datomic :as ddb]
            [immutant.messaging :as msg]
            [immutant.messaging.hornetq :as hornetq]
            [taoensso.timbre :as log]
            [immutant.cache :as cache]
            [dataproc.util :refer [gen-uuid]])
  (:import  [java.util.concurrent Executors]
            [org.hornetq.api.jms.management JMSQueueControl]))

(def ^:private tpool (Executors/newFixedThreadPool 4))

(def ^:private dcache (cache/lookup-or-create "dbscanner" :tx false :persist "/app/dataproc/cache"))

(defn- remove-worker-from-cache
  "Removes the information for a DBScanner worker from the cache"
  [id]
  (dosync
    (cache/swap! dcache :scanners disj id)
    (cache/delete dcache (keyword id))))

(defn workFn
  [params]
  (let [{:keys [id last-ref end-ref]} params
       entids (map :e (ddb/index-datoms :artist/name last-ref))]
    (msg/with-connection {}
      (doseq [entid (take-while (partial not= end-ref) entids)]
          (msg/publish "/queue/dataproc/work/" entid)
          (cache/swap! dcache (keyword id) assoc :last-ref entid)))
    (remove-worker-from-cache id)))

(defn- generate-work-params
  [start-ref]
  (let [entids (map :e (take 10000 (ddb/index-datoms :artist/name start-ref)))
       next (last entids)]
    (cache/put dcache :next-ref next)
    {:start-ref start-ref
     :end-ref next
     :last-ref start-ref}))

(defn- active-scanners
  "Doesn't quite work right yet"
  []
  (loop [scanners (get dcache :scanners)
         result {}]
    (if (empty? scanners)
      result
      (recur (rest scanners) (conj result (get dcache (keyword (first scanners))))))))

(defn- spawn-scanner
  []
  (let [params (generate-work-params (get dcache :next-ref))
        uuid (gen-uuid)]
    (dosync
      (cache/put dcache (keyword uuid) params)
      (cache/swap! dcache :scanners conj uuid)
      (workFn (conj params {:id uuid})))))
    

(defrecord DBScanner []
  daemon/Daemon
  (start [_]
    (cache/put-if-absent dcache :scanners #{})
    (log/info "Starting DBScanner")
    (.submit tpool spawn-scanner)
    (.submit tpool spawn-scanner)
    (loop []
      (println (.countMessages (hornetq/destination-controller "/queue/dataproc/work/") nil))
      (Thread/sleep 2000)
      (recur)))
  (stop [_]
    (.shutdown tpool)))
