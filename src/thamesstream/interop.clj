(ns thamesstream.interop
  (:require [clojure.string :refer [upper-case]]))

i;; (.methodName object)
(.toString Object)

(.indexOf "Let's synergize our bleeding edges" "y")

;; create new object, same as (new String)
(String.)

(System/getProperty "user.dir")

; reify

(def f "hello")



(reify clojure.lang.Seqable
  (seq [this] (upper-case f)))
