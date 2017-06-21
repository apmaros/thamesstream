(ns thamesstream.config
  (:import java.lang.System
           org.apache.kafka.common.serialization.Serdes
           org.apache.kafka.streams.StreamsConfig))

(def ^:private string-serde->str
  (.getName (.getClass (Serdes/String))))

(def  broker-address
  (or (System/getenv "KAFKA_BOOTSTRAP_SERVERS") "localhost:9092"))

(def zk-connect-string
  (or (System/getenv "ZK_CONNECT_STRING") "localhost:2181"))

(defn streams-props
  [application-id]
  {StreamsConfig/APPLICATION_ID_CONFIG    application-id
   StreamsConfig/BOOTSTRAP_SERVERS_CONFIG broker-address
   StreamsConfig/KEY_SERDE_CLASS_CONFIG   string-serde->str
   StreamsConfig/VALUE_SERDE_CLASS_CONFIG string-serde->str})
