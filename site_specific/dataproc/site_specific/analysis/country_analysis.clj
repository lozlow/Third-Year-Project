(ns dataproc.site-specific.analysis.country-analysis
  (:require [clojure.string :as str]
            [taoensso.timbre :as log])
  (:use     [clojure.string :only (lower-case split)]
            [dataproc.site-specific.db.core]))

; Pure functions

(defn strip-punctuation
  "http://stackoverflow.com/questions/18830813/how-can-i-remove-punctuation-from-input-text-in-java"
  [str]
  (str/replace str #"[^a-zA-Z\s]" ""))

(defn split-words
  [str]
  (split str #"\s+"))

(defn analyse-string
  [^String str]
  (-> str
    .toLowerCase
    strip-punctuation
    split-words
    frequencies))

(defn merge-freq
  "Merges two results from the 'frequencies' function"
  [coll freq-map]
  (let [entries (seq freq-map)
        entry (first entries)]
    (if (not (empty? freq-map))
      (if (contains? coll (key entry))
        (recur (assoc coll (key entry) (+ (get coll (key entry)) (val entry))) (into {} (rest entries)))
        (recur (conj coll entry) (into {} (rest entries))))
      coll)))

(defn most-frequent-n [coll n]
  "http://stackoverflow.com/questions/12657566/idiomatic-clojure-way-to-find-most-frequent-items-in-a-seq"
  (->> coll
    (sort-by val)
    reverse
    (take n)))

; Impure

(defn top-n-song-words-for-country
  [n country]
  (log/info "DEBUG: starting analysis")
  (log/info (most-frequent-n (reduce merge-freq 
                                     (pmap analyse-string
                                           (flatten (vec (tracks-for-country country))))) n)))

; Run pmap frequencies on datomic query
; Run reduce merge-freq
; Add results to database