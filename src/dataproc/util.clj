(ns dataproc.util
  (:require [clojure.string :as str]))

(defn require-fn 
  "Takes a string of the form \"namespace/fn\",
   requires the namespace, and returns the fn

   @reference https://github.com/immutant/immutant/blob/master/modules/core/src/main/clojure/immutant/runtime.clj"
  [namespaced-fn]
  (let [[namespace function] (map symbol (str/split namespaced-fn #"/"))]
    (require namespace)
    (intern namespace function)))

(defn gen-uuid
  "Generates a new UUID"
  []
  (str (java.util.UUID/randomUUID)))