(ns thames-stream.stores
  (:require [thamesstream.kstreams :as k]
            [thamesstream.edn-serde :refer [edn-serde]])
  (:import org.apache.kafka.common.serialization.Serdes
           org.apache.kafka.streams.state.QueryableStoreTypes))

;; GlobalKTable is an abstraction of a changelog stream, where each data record represents an update.

;; GlobalKTable is different from a KTable in that it is fully replicated on each KafkaStreams instance

;; GlobalKTable also provides the ability to look up current values of data records by keys

(comment

  (def builder
    (k/kstream-builder))

  (def builder2
    (k/kstream-builder))

  (def source1
    (.stream builder (into-array String ["topic1" "topic2"])))

  (def source2
    (.table builder "topic3" "state-store-3"))

  (def source3
    (.globalTable builder (Serdes/String) (edn-serde)  "topic4" "state-store-4"))
(def source4
    (.globalTable builder2 (Serdes/String) (edn-serde)  "topic4" "state-store-4"))

  (def streams
    (k/kafka-streams builder (k/streams-config "global-store")))

  (def streams1
    (k/kafka-streams builder (k/streams-config "global-store-1")))

  (.start streams1)

  (.close streams)

  (def view
    (.store streams "state-store-4" (QueryableStoreTypes/keyValueStore)))

  (def view1
    (.store streams1 "state-store-4" (QueryableStoreTypes/keyValueStore)))

  (.stateStoreNameToSourceTopics builder)

  (.nodeGroups builder)

 (.latestResetTopicsPattern builder)

  (.earliestResetTopicsPattern builder)

  (k/all view)

  (k/all view1)

  (map (fn [e] {:key (.key e)
               :value (.value e)})
       (iterator-seq (.all view)))


  )
