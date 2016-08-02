(ns thamesstream.joiners
  (:require [clojure.string :refer [split]]
            [taoensso.timbre :as log])
  (:import org.apache.kafka.common.serialization.Serdes
           [org.apache.kafka.streams KafkaStreams KeyValue StreamsConfig]
           [org.apache.kafka.streams.kstream KeyValueMapper KStreamBuilder Reducer ValueJoiner ValueMapper]))

(def properties
  {StreamsConfig/APPLICATION_ID_CONFIG,    "join-users"
   StreamsConfig/BOOTSTRAP_SERVERS_CONFIG, (or (System/getenv "KAFKA_BOOTSTRAP_SERVERS")
                                               "localhost:9092")
   StreamsConfig/KEY_SERDE_CLASS_CONFIG,   (.getName (.getClass (Serdes/String)))
   StreamsConfig/VALUE_SERDE_CLASS_CONFIG, (.getName (.getClass (Serdes/String)))})

(def config
  (StreamsConfig. properties))

(def builder
  (KStreamBuilder.))

(def clicks-topic
  (into-array String ["clicks"]))

(def regions-topic
  "regions")

(def sink-topic
  "counts")

(def clicks-stream
  (.stream builder clicks-topic))

(def regions-table
  (.table builder regions-topic))

(def clicks-per-region
  (->
   clicks-stream
   (.leftJoin
    regions-table
    (reify ValueJoiner (apply [this clicks region]
                         {:region (or region
                                      "UNKNOWN") :clicks clicks})))
   (.map
    (reify KeyValueMapper (apply [_ user region-with-clicks]
                            (log/debug (format "user %s joined with region-clicks %s "
                                               user region-with-clicks))
                            (KeyValue. (:region region-with-clicks)
                                       (:clicks region-with-clicks)))))
   (.reduceByKey (reify Reducer (apply [this first-clicks second-clicks]
                                  (log/debug (log/debug "previous clicks %s, current clicks %s "
                                             first-clicks second-clicks))
                                  (str (+ (read-string first-clicks)
                                     (read-string second-clicks)))))
                 "clicks-per-region-unwindowed")
   ;(.print)
   (.to (Serdes/String) (Serdes/String) "regions-clicks")
   ))

(def stream
  (KafkaStreams. builder config))

(defn start-stream
  []
  (.start stream)

  )

(defn -main
  []
  (start-stream)
  (.close stream)

  (println "Hello, World!"))

