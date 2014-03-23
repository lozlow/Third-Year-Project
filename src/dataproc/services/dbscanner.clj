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

(def ^:private tpool (Executors/newFixedThreadPool 4))
(def ^:private num-publish-threads (config/get :dbscanner-publish-threads))
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
       entids (take-while (partial not= end-ref) (map :e (ddb/index-datoms (config/get :dbscanner-scan-index) last-ref)))]
    (msg/with-connection {}
      (doseq [entid entids]
          (msg/publish "/queue/dataproc/work/" entid)
          (ascache/swap! dcache (keyword id) assoc :last-ref entid)))
    (remove-worker-from-cache id)))

(defn- generate-work-params
  "This is NOT thread safe and should ONLY be called on a single thread"
  [start-ref]
  (let [entids (map :e (take 5000 (ddb/index-datoms :artist/name start-ref)))
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
  (while (true? @running)
    (log/report {:work-msgqueue-size (.countMessages (hornetq/destination-controller "/queue/dataproc/work/") nil)
                 :running-workers-in-cache (count (active-scanners))
                 :running-worker-threads-in-pool (.getActiveCount tpool)})
    (Thread/sleep 10000)))

(defn init
  []
  (def dcache (cache/get-cache "dbscanner"))
  (ascache/put-if-absent dcache :scanners #{}))

(defrecord DBScanner []
  daemon/Daemon
  (start [_]
    (reset! running true)
    (log/info "Starting DBScanner")
    (let [num-active-scanners (count (active-scanners))]
      (if (>= num-active-scanners 4)
        (do
          (log/info "Resuming" num-active-scanners "active scanners")
          (resume-scanners tpool (:scanners dcache)))
        (do
          (log/info "Resuming" num-active-scanners "active scanners and spawning" (- 4 num-active-scanners) "additional scanners")
          (resume-scanners tpool (:scanners dcache))
          (spawn-scanners tpool (- 4 num-active-scanners)))))
    (.start (Thread. stats-fn))
    (while (true? @running)
      (let [num-active-scanners (count (active-scanners))]
        (when (< num-active-scanners 4)
          (log/info "Spawning" (- 4 num-active-scanners) "additional scanners")
          (spawn-scanners tpool (- 4 num-active-scanners))))
      (Thread/sleep 30000)))
  (stop [_]
    (reset! running false)
    (.shutdown tpool)))
