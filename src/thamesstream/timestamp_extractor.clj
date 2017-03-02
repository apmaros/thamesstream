(ns thamesstream.timestamp-extractor
  (:import org.apache.kafka.streams.processor.TimestampExtractor))

(deftype TimestamptExtractor []
  TimestampExtractor
  (extract [_ message]
    (let [message-timestamp  (:published-at (.value message))]
      (when message-timestamp
        (prn "Overriding timestamp from metadata with message timestamp: "
             message-timestamp))
      (or message-timestamp (.timestamp message)))))
