(ns site-specific.t-core
  (:require [dataproc.site-specific.analysis.core :refer :all])
  (:use     [midje.sweet]))

(fact "`is-collab?` returns true when an artist collaborates on a song"
  (is-collab? "My great song") => false
  (is-collab? "My great song feat. Dr Dre") => true
  (is-collab? "My great song featuring The Killers") => true
  (is-collab? "My great song FEAT The Killers") => true)