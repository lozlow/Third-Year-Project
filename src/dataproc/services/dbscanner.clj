(ns dataproc.services.dbscanner
  (:require [immutant.daemons :as daemon]
            [dataproc.config :as config]
            [datomic.api :as d]
            [dataproc.db.datomic :as ddb]
            [immutant.messaging :as msg]
            [immutant.messaging.hornetq :as hornetq]
            [taoensso.timbre :as log]
            [dataproc.cache.core :as cache]
            [immutant.cache :as ascache]
            [dataproc.util :refer [gen-uuid]])
  (:import  [java.util.concurrent Executors]
            [org.hornetq.api.jms.management JMSQueueControl]))

(def ^:private running (atom false))

(declare ^:private num-max-publish-threads)
(declare ^:private tpool)
(declare ^:private dcache)

(defn- remove-worker-from-cache
  "Removes the information for a DBScanner worker from the cache"
  [id]
  (dosync
    (ascache/swap! dcache :scanners disj id)
    (ascache/delete dcache (keyword id))))

(defn workFn
  [params]
  (let [{:keys [id last-ref end-ref]} params
        {:keys [msgserv]} (config/get-config :work-queue)
       entids (take-while (partial not= end-ref) (map :e (ddb/index-datoms (config/get-config :dbscanner-scan-index) last-ref)))]
    (msg/with-connection {}
      (doseq [entid entids]
          (msg/publish msgserv entid)
          (ascache/swap! dcache (keyword id) assoc :last-ref entid)))
    (remove-worker-from-cache id)))

(defn- generate-work-params
  "This is NOT thread safe and should ONLY be called on a single thread"
  [start-ref]
  (let [entids (map :e (take 5000 (ddb/index-datoms (config/get-config :dbscanner-scan-index) start-ref)))
       next (last entids)]
    (ascache/put dcache :next-ref next)
    {:start-ref start-ref
     :end-ref next
     :last-ref start-ref}))

(defn- active-scanners
  "Returns the list of active scanners"
  []
  (loop [scanners (get dcache :scanners)
         result {}]
    (if (empty? scanners)
      result
      (recur (rest scanners) (assoc result (keyword (first scanners)) (get dcache (keyword (first scanners))))))))

(defn- create-scanner-with-params
  []
  (let [params (generate-work-params (get dcache :next-ref))
        uuid (gen-uuid)]
    (dosync
      (ascache/put dcache (keyword uuid) params)
      (ascache/swap! dcache :scanners conj uuid)
      #(workFn (assoc params :id uuid)))))

(defn- spawn-scanners
  ([tpool]
    (let [scanner (create-scanner-with-params)]
    (.submit tpool scanner)))
  ([tpool num]
    (dotimes [n num]
        (.submit tpool (create-scanner-with-params)))))

(defn- resume-scanners
  [tpool scanner-ids]
  (doseq [key scanner-ids]
    (let [params (get dcache (keyword key))]
      (.submit tpool #(workFn (assoc params :id key))))))

(defn ^:private stats-fn
  []
  (let [msgcontrol (hornetq/destination-controller (:msgserv (config/get-config :work-queue)))]
    (while (true? @running)
      (log/report {:work-msgqueue-size (.countMessages msgcontrol nil)
                   :running-workers-in-cache (count (active-scanners))
                   :running-worker-threads-in-pool (.getActiveCount tpool)})
      (Thread/sleep 10000))))

(defn init
  []
  (def dcache (cache/get-cache "dbscanner"))
  (def num-max-publish-threads (config/get-config :dbscanner-publish-threads))
  (def tpool (Executors/newFixedThreadPool num-max-publish-threads))
  (ascache/put-if-absent dcache :scanners #{}))

(defrecord DBScanner []
  daemon/Daemon
  (start [_]
    (reset! running true)
    (log/info "Starting DBScanner")
    (let [num-active-scanners (count (active-scanners))
          delta (- num-max-publish-threads num-active-scanners)]
      (if (>= num-active-scanners num-max-publish-threads)
        (do
          (log/info "Resuming" num-active-scanners "active scanners")
          (resume-scanners tpool (:scanners dcache)))
        (do
          (log/info "Resuming" num-active-scanners "active scanners and spawning" delta "additional scanners")
          (resume-scanners tpool (:scanners dcache))
          (spawn-scanners tpool delta))))
    (.start (Thread. stats-fn))
    (while (true? @running)
      (let [num-active-scanners (count (active-scanners))
            delta (- num-max-publish-threads num-active-scanners)]
        (when (< num-active-scanners num-max-publish-threads)
          (log/info "Spawning" delta "additional scanners")
          (spawn-scanners tpool delta)))
      (Thread/sleep 30000)))
  (stop [_]
    (reset! running false)
    (.shutdown tpool)))
