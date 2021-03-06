(ns dataproc.util
  (:require [clojure.string :as str])
  (:use     [immutant.util :only [app-relative]]))

(defn require-fn 
  "Takes a string of the form \"namespace/fn\",
   requires the namespace, and returns the fn

   @reference https://github.com/immutant/immutant/blob/master/modules/core/src/main/clojure/immutant/runtime.clj"
  [namespaced-fn]
  (let [[namespace function] (map symbol (str/split namespaced-fn #"/"))]
    (require namespace)
    (intern namespace function)))

(defn app-path
  "path - the relative path

   With no input this function returns the path to this application."
  ([]
    (.getPath (app-relative)))
  ([path]
    (if (.startsWith path "/")
      (str (.getPath (app-relative)) path)
      (str (.getPath (app-relative)) "/" path))))

(defn gen-uuid
  "Generates a new UUID"
  []
  (str (java.util.UUID/randomUUID)))

(defn vecmap-to-separated-string
  [coll & [separator]]
  (->> coll
	  (map name)
	  (partition 2)
	  (map (partial str/join " "))
	  (str/join (or separator ", "))))
