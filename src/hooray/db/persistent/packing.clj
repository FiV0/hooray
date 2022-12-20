(ns hooray.db.persistent.packing
  (:refer-clojure :exclude [hash])
  (:require [taoensso.nippy :as nippy])
  (:import (java.util Arrays)
           (java.nio ByteBuffer)))

;; TODO use ByteBuffer slice and wrap to not copy anything and work with raw arrays

(defn ->buffer [v] (nippy/freeze v))
(defn ->value [b] (nippy/thaw b))

(def ^:private hash-length (Integer/BYTES))
(defn hash [o] (clojure.core/hash o))

(defn bytes->int [^"[B" b]
  (.getInt (ByteBuffer/wrap b)))

(defn int->bytes [^Integer i]
  (.. (ByteBuffer/allocate 4) (putInt i) array))

(comment
  (-> (hash {}) int->bytes bytes->int))

(defn pack-hash-array [& args]
  (let [len (* hash-length (count args))
        ba (byte-array len)]
    (->> (map hash args)
         (reduce (fn [i o]
                   (System/arraycopy (int->bytes o) 0 ba i hash-length)
                   (+ i hash-length))
                 0))
    ba))

(defn unpack-hash-array [^"[B" b]
  (loop [res [] i 0]
    (if (< i (count b))
      (let [new-i (+ i hash-length)]
        (recur (conj res (bytes->int (Arrays/copyOfRange b i new-i))) new-i))
      res)))

(comment
  (def some-values [{} 1 3])
  (map hash some-values)
  ;; => (-15128758 1392991556 -1556392013)
  (-> (apply pack-hash-array some-values)
      unpack-hash-array)
  )
