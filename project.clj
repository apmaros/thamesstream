(defproject thamesstream "0.1.0-SNAPSHOT"
  :description "FIXME: write description"

  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}

  :main thamesstream.core

  :uberjar-name "thamesstream.jar"

  :repositories {"confluent" {:url "http://packages.confluent.io/maven/"}}

  :dependencies [[org.apache.kafka/kafka-streams "0.10.0.0-cp1"]
                 [org.clojure/clojure "1.8.0"]])
