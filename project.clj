(defproject thamesstream "0.1.0-SNAPSHOT"
  :description "FIXME: write description"

  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}

  :main thamesstream.core

  :aot [thamesstream.timestamp-extractor]

  :uberjar-name "thamesstream.jar"

  :repositories {"confluent" {:url "http://packages.confluent.io/maven/"}}

  :dependencies [[org.clojure/clojure "1.8.0"]
                 [com.taoensso/timbre "4.7.0"]
                 [samza-avro-confluent-serde "0.1.5"]
                 [org.apache.kafka/kafka-streams "0.10.2.0"]
                 [org.apache.kafka/kafka_2.11 "0.10.2.0"]
                 [org.apache.kafka/kafka-clients "0.10.2.0"]
                 [clj-time "0.12.2"]])
