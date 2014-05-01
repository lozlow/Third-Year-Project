(ns dataproc.analysis.core)

(defn nil-or-false?
  [expr]
  (or (nil? expr)
      (false? expr)))

(defn rule
  [name func]
  {:name name :func func})

(defn ruleset
  [val matchfn & rules]
  (loop [matches #{}
         rules rules]
    (if (not (empty? rules))
      (let [{:keys [name func]} (first rules)
            fnres (func val)]
        (if (not (nil-or-false? fnres))
          (recur (conj matches name) (rest rules))
          (recur matches (rest rules))))
      (when-not (empty? matches)
        (apply (partial matchfn val) matches)))))