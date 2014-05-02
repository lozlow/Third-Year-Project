(ns dataproc.t-util
  (:require [dataproc.util :refer :all])
  (:use     [midje.sweet]))

(fact "`mapvals-to-separated-string` returns a string separated by ', ' another string given a map"
  (vecmap-to-separated-string '{}) => ""
  (vecmap-to-separated-string '{:one :two}) => "one two"
  (vecmap-to-separated-string '{:one :two :three :four}) => "one two, three four"
  
  (vecmap-to-separated-string '{} ":") => ""
  (vecmap-to-separated-string '{:one :two} ":") => "one two"
  (vecmap-to-separated-string '{:one :two :three :four} ":") => "one two:three four")