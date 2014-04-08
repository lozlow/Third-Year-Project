(ns dataproc.site-specific.analysis.genre-analysis
  (:require [clojure.string :as str])
  (:use     [clojure.string :only (lower-case split)]))

(defn bracket-value
  [str]
  (let [start (+ 1 (.indexOf str "("))
        end (.indexOf str ")")
        substr (subs str start end)]
    substr))

(defn replace-bracket
  [str]
  (str/replace str #"\([a-z]*\)" (bracket-value str)))

(defn split-words
  [str]
  (split str #"\s+"))

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

; Run pmap frequencies on datomic query
; Run reduce merge-freq
; Add results to database