(ns dataproc.t-core
  (:require [dataproc.app :refer :all]
            [clojure.string :as str])
  (:use     [midje.sweet]))

(fact "`split` splits strings on regular expressions and returns a vector"
  (str/split "a/b/c" #"/") => ["a" "b" "c"]
  (str/split "" #"irrelevant") => [""]
  (str/split "no regexp matches" #"a+\s+[ab]") => ["no regexp matches"])