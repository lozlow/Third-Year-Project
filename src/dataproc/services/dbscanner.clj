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
      (cache/put dcache (keyword uuid) params)
      (cache/swap! dcache :scanners conj uuid)
      #(workFn (assoc params :id uuid)))))

(defn- spawn-scanners
  ([tpool]
    (let [scanner (create-scanner-with-params)]
    (.submit tpool scanner)))
  ([tpool num]
    (dotimes [n num]
        (.submit tpool (create-scanner-with-params)))))

(defn- resume-scanners
  [tpool scanner-params]
  (for [key scanner-params
        :let [params (get dcache (keyword key))]]
    (.submit tpool #(workFn (assoc params :id key)))))

(defrecord DBScanner []
  daemon/Daemon
  (start [_]
    (cache/put-if-absent dcache :scanners #{})
    (log/info "Starting DBScanner")
    (let [active-scanners (active-scanners)
          num-active-scanners (count active-scanners)]
      (if (>= num-active-scanners 4)
        (do
          (log/info "Resuming" num-active-scanners "active scanners")
          (resume-scanners tpool (:scanners dcache)))
        (do
          (log/info "Resuming" num-active-scanners "active scanners and spawning" (- 4 num-active-scanners) "additional scanners")
          (resume-scanners tpool (:scanners dcache))
          (spawn-scanners tpool (- 4 num-active-scanners)))))
    (loop []
      (log/report "Stats:" (.countMessages (hornetq/destination-controller "/queue/dataproc/work/") nil))
      (Thread/sleep 10000)
      (recur)))
  (stop [_]
    (.shutdown tpool)))
