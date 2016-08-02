(ns thamesstream.utils)

(def stream-store (atom nil))

(defn set-stream
  [stream]
  (swap! stream (constantly stream)))
