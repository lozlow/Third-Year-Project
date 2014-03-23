(ns dataproc.config
  (:require [clojure.java.io :as io]
            [clojure.edn :as edn]
            [taoensso.timbre :as log])
  (:import  [java.io PushbackReader FileNotFoundException]))

(def ^:private config (atom {}))

(defn- load-defaults
  "Loads default configuration values"
  [])

(defn- read-edn
  "Reference http://stackoverflow.com/questions/7777882/loading-configuration-file-in-clojure-as-data-structure"
  [cfgfile]
  (try
    (with-open [r (io/reader cfgfile)]
      (edn/read (PushbackReader. r)))
    (catch FileNotFoundException e
      (log/error (str "Could not load file: " cfgfile)))))

(defn load-config
  [cfgfile]
  (reset! config (read-edn cfgfile)))

(defn get-config
  "When called with no parameters this function returns the entire
   configuration map.

   When called with a single keyword parameter this function returns
   the configuration value for that keyword."
  ([]
    (deref config))
  ([keyword]
    (get (deref config) keyword)))