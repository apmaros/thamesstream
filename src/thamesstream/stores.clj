(ns thamesstream.stores)

;; GlobalKTable is an abstraction of a changelog stream, where each data record represents an update.

;; GlobalKTable is different from a KTable in that it is fully replicated on each KafkaStreams instance

;; GlobalKTable also provides the ability to look up current values of data records by keys


