(defproject neb "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :source-paths ["src/clj"]
  :java-source-paths ["src/java"]
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [aleph "0.4.1-beta2"]
                 [cluster-connector "0.1.0-SNAPSHOT"]
                 [net.openhft/koloboke-api-jdk8 "0.6.8"]
                 [net.openhft/koloboke-impl-jdk8 "0.6.8"]])
