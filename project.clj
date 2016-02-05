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
                 [net.openhft/koloboke-impl-jdk8 "0.6.8"]]
  :plugins [[lein-midje "3.1.3"]]
  :profiles {:uberjar {:aot :all}
             :dev {:dependencies [[midje "1.8.3"]
                                  [com.github.jbellis/jamm "0.3.1"]]}}
  :jvm-opts [;"-Djava.rmi.server.hostname=<hostname>" ;;add this when remote-connect fail
             ;"-Dcom.sun.management.jmxremote"
             ;"-Dcom.sun.management.jmxremote.port=9876"
             ;"-Dcom.sun.management.jmxremote.authenticate=false"
             ;"-Dcom.sun.management.jmxremote.ssl=false"
             "-javaagent:/Users/shisoft/.m2/repository/com/github/jbellis/jamm/0.3.1/jamm-0.3.1.jar"
             ;"-Xmx8g"
             "-XX:+UseParNewGC" "-XX:+UseConcMarkSweepGC" "-XX:+CMSParallelRemarkEnabled"])
