(ns site-specific.t-country-analysis
  (:require [dataproc.site-specific.analysis.countr-analysis :refer :all])
  (:use     [midje.sweet]))

(fact "`strip-punctuation` strips a string of all punctuation (and numbers)"
  (strip-punctuation "My great song") => "My great song"
  (strip-punctuation "My great song5") => "My great song"
  (strip-punctuation "My great song feat. Dr Dre") => "My great song feat Dr Dre"
  (strip-punctuation "My great song (feat. Dr Dre)") => "My great song feat Dr Dre"
  (strip-punctuation "My great song(s)") => "My great songs")

(fact "`split-words` splits a string into a list of words"
  (split-words "My") => ["My"]
  (split-words "My great song") => ["My" "great" "song"])

(fact "`merge-freq` merges the result of two frequencies maps")
; Todo tests