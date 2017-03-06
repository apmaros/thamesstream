(ns thamesstream.play
  (:require [taoensso.timbre :as log])
  (:import org.apache.kafka.common.serialization.Serdes
           [org.apache.kafka.streams KafkaStreams StreamsConfig]
           [org.apache.kafka.streams.kstream Aggregator Initializer JoinWindows KStreamBuilder ValueJoiner]
           [org.apache.kafka.streams.integration.utils.EmbeddedSingleNodeKafkaCluster]
           org.apache.kafka.streams.kstream.Initializer))

(def properties
  {StreamsConfig/APPLICATION_ID_CONFIG,    "instructions"
   StreamsConfig/BOOTSTRAP_SERVERS_CONFIG, (or (System/getenv "KAFKA_BOOTSTRAP_SERVERS")
                                               "localhost:9092")
   StreamsConfig/KEY_SERDE_CLASS_CONFIG,   (.getName (.getClass (Serdes/String)))
   StreamsConfig/VALUE_SERDE_CLASS_CONFIG, (.getName (.getClass (Serdes/String)))})

(def config
  (StreamsConfig. properties))

(def builder
  (KStreamBuilder.))

(def instruction-topic
  (into-array String ["instruction"]))

(def results-topic
  (into-array String ["results"]))

(def instruction-stream
  (.stream builder instruction-topic))

(def results-stream
  (.stream builder results-topic))


(def instruction-processed-stream
  (->
   instruction-stream
   (.leftJoin results-stream
              (reify ValueJoiner (apply [this instruction result]
                                   (str {:instruction (or instruction "UNKNOWN") :result result})))
              (JoinWindows/of "instructions"))))

(.print instruction-processed-stream)
(.to instruction-processed-stream "resolution")

(.before (.within (JoinWindows/of "instructions") 1000000))

(-> instruction-processed-stream
    (.countByKey "counter")
    (.print))

(-> instruction-processed-stream
    (.writeAsText "resources/out.txt"))


(-> instruction-processed-stream
    (.aggregateByKey (reify Initializer (apply [this] 0))
                     (reify Aggregator (apply [this key value aggregate]
                                         (log/debug (format "Aggregated %s/%s %s"
                                                            key value aggregate))))
                     "inst-aggregated"))

(def stream
  (KafkaStreams. builder config))

(defn start-stream
  []
  (.start stream)

  )

(comment

  (.close stream)

  )
