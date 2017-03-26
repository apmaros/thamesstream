(ns thamesstream.config
  (:import org.apache.kafka.common.serialization.Serdes
           org.apache.kafka.streams.StreamsConfig))

(def ^:private string-serde->str
  (.getName (.getClass (Serdes/String))))

(def  broker-address
  (or (System/getenv "KAFKA_BOOTSTRAP_SERVERS") "localhost:9092"))

(defn streams-props
  [application-id]
  {StreamsConfig/APPLICATION_ID_CONFIG    application-id
   StreamsConfig/BOOTSTRAP_SERVERS_CONFIG broker-address
   StreamsConfig/KEY_SERDE_CLASS_CONFIG   string-serde->str
   StreamsConfig/VALUE_SERDE_CLASS_CONFIG string-serde->str})
