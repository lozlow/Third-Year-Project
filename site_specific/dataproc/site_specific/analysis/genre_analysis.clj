(ns dataproc.site-specific.analysis.genre-analysis
  (:require [clojure.string :as str])
  (:use     [clojure.string :only (lower-case split)]))

; Pure functions

(defn strip-punctuation
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

; Impure

(reduce merge-freq
        (pmap analyse-string
              '("Hello my name is pete" "so doge" "Hello I wish you know you my name")))

; Run pmap frequencies on datomic query
; Run reduce merge-freq
; Add results to database