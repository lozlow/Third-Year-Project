(defproject dataproc "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :repositories {"my.datomic.com" {:url "https://my.datomic.com/repo"
                                 :username "peeta.shaw@gmail.com"
                                 :password "39eb876d-aaf2-4343-8fa2-e02e919daaa7"}}
  :source-paths ["src" "site_specific"]
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.immutant/immutant "1.1.0"]
                 [com.datomic/datomic-pro "0.9.4556"
                  :exclusions [com.fasterxml.jackson.core/jackson-annotations
                               com.fasterxml.jackson.core/jackson-core
                               com.fasterxml.jackson.core/jackson-databind]]
                 [com.taoensso/timbre "3.0.0"]]
  :immutant {:nrepl-port 4335
             :init "dataproc.app/init"}
  :jvm-opts ^:replace ["-Xmx1g" "-server"])