(ns hooray.db.persistent.packing
  (:refer-clojure :exclude [hash])
  (:require [taoensso.nippy :as nippy])
  (:import (java.util Arrays)
           (java.nio ByteBuffer)))

;; TODO use ByteBuffer slice and wrap to not copy anything and work with raw arrays
;; TODO look at https://gist.github.com/pingles/1235344 for inspiration

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

;; LOOK for more efficient implementation
(defn inc-ba
  ([^"[B" b] (inc-ba b (dec (count b))))
  ([^"[B" b idx]
   (let [val (aget b idx)]
     (cond (and (= 0 idx) (= val Byte/MAX_VALUE))
           (throw (ex-info "Byte Array overflow!" {}))
           (< val Byte/MAX_VALUE)
           (do
             (aset-byte b idx (byte (inc val)))
             b)
           :else
           (do
             (aset-byte b idx (byte 0))
             (recur b (dec idx)))))))

(defn copy [^"[B" b]
  (let [res (byte-array (count b))]
    (System/arraycopy b 0 res 0 (count b))
    res))

(comment
  (-> 1 int->bytes bump-ba  bytes->int)
  (-> 127 int->bytes vec)
  (-> (byte-array [0 0 0 127]) bump-ba vec)
  (-> (byte-array [0 0 0 127]) bump-ba bytes->int))

;; !important! this takes already hashes
(defn pack-hash-array [& args]
  (let [len (* hash-length (count args))
        ba (byte-array len)]
    (reduce (fn [i o]
              (System/arraycopy (int->bytes o) 0 ba i hash-length)
              (+ i hash-length))
            0
            args)
    ba))

(defn unpack-hash-array
  "version that also converts to comparable hashes."
  [^"[B" b]
  (loop [res [] i 0]
    (if (< i (count b))
      (let [new-i (+ i hash-length)]
        (recur (conj res (bytes->int (Arrays/copyOfRange b i new-i))) new-i))
      res)))

(defn unpack-hash-array*
  "version that keeps raw byte arrays"
  [^"[B" b]
  (loop [res [] i 0]
    (if (< i (count b))
      (let [new-i (+ i hash-length)]
        (recur (conj res (Arrays/copyOfRange b i new-i)) new-i))
      res)))

(defn concat-ba [^"[B" b1 ^"[B" b2]
  (let [len (+ (count b1) (count b2))
        res (byte-array len)]
    (System/arraycopy b1 0 res 0 (count b1))
    (System/arraycopy b2 0 res (count b1) (count b2))
    res))

(comment
  (def some-values [{} 1 3])
  (map hash some-values)
  ;; => (-15128758 1392991556 -1556392013)
  (->>
   (map hash some-values)
   (apply pack-hash-array)
   unpack-hash-array)
  )
