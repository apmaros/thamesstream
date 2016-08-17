(defproject thamesstream "0.1.0-SNAPSHOT"
  :description "FIXME: write description"

  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}

  :main thamesstream.core

  :uberjar-name "thamesstream.jar"

  :repositories {"confluent" {:url "http://packages.confluent.io/maven/"}}

  :dependencies [[org.clojure/clojure "1.8.0"]
                 [com.taoensso/timbre "4.7.0"]
                 [samza-avro-confluent-serde "0.1.5"]
                 [org.apache.kafka/kafka-streams "0.10.0.0-cp1"]
                 [org.apache.kafka/kafka-clients "0.10.0.0-cp1"]
                 [io.confluent/kafka-avro-serializer "3.0.0"]
                 [org.apache.avro/avro "1.7.7"]
                 [org.apache.avro/avro-maven-plugin "1.7.7"]
                 [io.confluent/streams-examples "3.0.0"  :exclusions [org.apache.avro/avro-maven-plugin]]])
