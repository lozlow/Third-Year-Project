(ns dataproc.t-util
  (:require [dataproc.util :refer :all])
  (:use     [midje.sweet]))

(fact "`mapvals-to-separated-string` returns a string separated by ', ' another string given a map"
  (mapvals-to-separated-string '{}) => ""
  (mapvals-to-separated-string '{:one :two}) => "one two"
  (mapvals-to-separated-string '{:one :two :three :four}) => "one two, three four"
  
  (mapvals-to-separated-string '{} ":") => ""
  (mapvals-to-separated-string '{:one :two} ":") => "one two"
  (mapvals-to-separated-string '{:one :two :three :four} ":") => "one two:three four")