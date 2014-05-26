(defproject dataproc "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :repositories {"my.datomic.com" {:url "https://my.datomic.com/repo"
                                 :username "YOUR-USERNAME"
                                 :password "YOUR-DOWNLOAD-KEY"}}
  :source-paths ["src" "site_specific"]
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.immutant/immutant "1.1.0"]
                 [com.datomic/datomic-pro "0.9.4755"
                  :exclusions [com.fasterxml.jackson.core/jackson-annotations
                               com.fasterxml.jackson.core/jackson-core
                               com.fasterxml.jackson.core/jackson-databind]]
                 [com.taoensso/timbre "3.0.0"]
                 [org.clojure/java.jdbc "0.3.3"]
                 [postgresql/postgresql "9.1-901.jdbc4"]
                 [com.mchange/c3p0 "0.9.2.1"]]
  :profiles {:dev
               {:dependencies [[midje "1.6.2"]]}}
  :immutant {:nrepl-port 4335
             :init "dataproc.app/init"}
  :jvm-opts ^:replace ["-Xmx1g" "-server"])
