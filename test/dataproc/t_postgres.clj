(ns dataproc.t-postgres
  (:require [dataproc.db.postgres :refer :all])
  (:use     [midje.sweet]))

(fact "`gen-create-table-string` returns a string to create a SQL table if it does not already exist"
  (gen-create-table-string "tablename" '{}) => "CREATE TABLE IF NOT EXISTS tablename ()"
  (gen-create-table-string "tablename" '{"id1" :text}) => "CREATE TABLE IF NOT EXISTS tablename (id1 text)"
  (gen-create-table-string "tablename" '{"id1" :text "id2" :bigint}) => "CREATE TABLE IF NOT EXISTS tablename (id1 text, id2 bigint)")