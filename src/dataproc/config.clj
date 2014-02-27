(ns dataproc.config
  (:require [clojure.java.io :as io]
            [clojure.edn :as edn])
  (:import [java.io PushbackReader FileNotFoundException]))

(def ^:private config (atom {}))

(defn load-config
  "Reference http://stackoverflow.com/questions/7777882/loading-configuration-file-in-clojure-as-data-structure"
  [cfgfile]
  (try
    (with-open [r (io/reader cfgfile)]
      (reset! config (edn/read (PushbackReader. r))))
    (catch FileNotFoundException e
      (println "Could not load configuration file"))))

(defn get-config
  ([]
    (deref config))
  ([k]
    (k (deref config))))