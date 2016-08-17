(ns thamesstream.avro-integration
  (:import io.confluent.examples.streams.utils.SpecificAvroSerde
           io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
           org.apache.kafka.clients.producer.ProducerConfig
           org.apache.kafka.common.serialization.StringSerializer
           org.apache.kafka.streams.kstream.KStreamBuilder
           org.apache.kafka.streams.StreamsConfig
           org.apache.avro.Schema
           java.util.Properties))

(def props
  {
   ProducerConfig/BOOTSTRAP_SERVERS_CONFIG, (or (System/getenv "KAFKA_BOOTSTRAP_SERVERS")
                                               "localhost:9092")
   ProducerConfig/KEY_SERIALIZER_CLASS_CONFIG, (.getClass StringSerializer)
   ProducerConfig/VALUE_SERIALIZER_CLASS_CONFIG, (.getClass io.confluent.kafka.serializers.KafkaAvroSerializer)
   "schema.registry.url", (or (System/getenv "SCHEMA_REGISTRY_URL")
                                                                "localhost:9092")})

(def config
  (StreamsConfig. props))

(defn load-schema
  [path]
  (-> (slurp (format "resources/%s" path))
      (Schema.)))

(new Schema.Parser.parse(schema))

(org.apache.avro.Schema.Parser.)

(Properties.)

(def schema {})

(.parse Schema#parse Parser schema)

Schema/Parser

(def generic-record-builder
  (GenericRecordBuilder. (load-schema "pageview.avsc"))))

(def builder
  (KStreamBuilder.))
