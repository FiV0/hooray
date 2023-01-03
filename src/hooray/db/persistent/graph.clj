(ns hooray.db.persistent.graph
  (:require [clojure.spec.alpha :as s]
            [hooray.db.iterator :as itr]
            [hooray.db.persistent.packing :as pack]
            [hooray.db.persistent.protocols :as proto]
            [hooray.graph :as g]
            [hooray.query.spec :as h-spec]
            [hooray.util :as util]
            [taoensso.nippy :as nippy])
  (:import (java.nio ByteBuffer)))

(defn- ->value [b] (nippy/thaw b))

(declare get-from-index)
(declare ->redis-iterator)

;; TODO look at optimizing
(defn resolve-triple* [key-store doc-store triple]
  (->> (get-from-index key-store triple)
       (map (fn [triple-bytes] (->> triple-bytes
                                    (proto/get-kvs doc-store :doc-store)
                                    (mapv pack/->value))))
       doall))

(defrecord PersistentGraph [connection key-store doc-store]
  g/Graph
  (new-graph [this] (util/unsupported-ex))
  (new-graph [this _opts] (util/unsupported-ex))
  (graph-add [this _triple] (util/unsupported-ex))
  (graph-delete [this _triple] (util/unsupported-ex))
  (graph-transact [this _tx-id _assertions _retractions] (util/unsupported-ex))
  (transact [this _tx-data] (util/unsupported-ex))
  (transact [this _tx-data _ts] (util/unsupported-ex))

  ;; the memory use hash join doesn't hash anything
  ;; so we need to hash here
  ;; Should we add a dummy hash fn for in momory to decomplect things?
  ;; TODO add LRU cache here
  (resolve-triple [this triple] (resolve-triple* key-store doc-store triple))
  (resolve-triple [this _triple _ts] (util/unsupported-ex))

  g/GraphIndex
  (resolve-tuple [this tuple]
    (s/assert ::h-spec/tuple tuple)
    (util/unsupported-ex))
  (get-iterator [this tuple] (->redis-iterator tuple key-store))
  (get-iterator [this tuple _type] (->redis-iterator tuple key-store))

  (hash->value [this h] (-> (proto/get-kv doc-store :doc-store (cond-> h (pack/byte-buffer? h) pack/bb-unwrap))
                            ->value))
  (hashs->values [this hs]
    (when (seq hs)
      (->> (proto/get-kvs doc-store :doc-store
                          (cond->> hs
                            (pack/byte-buffer? (first hs)) (map pack/bb-unwrap)))
           (map ->value)))))

(defn ->persistent-graph [conn key-store doc-store]
  (->PersistentGraph conn key-store doc-store))

;; TODO optimize for not returning constant columns
(defn simplify [binding] (map #(if (util/variable? %) '? :v) binding))

(defmulti get-from-index (fn [_key-store binding] (simplify binding)))

(defmethod get-from-index '[? ? ?]
  [key-store [_ _ _]]
  (->> (proto/get-range key-store :eav)
       (map pack/unpack-hash-array*)))

(defmethod get-from-index '[? ? :v]
  [key-store [_ _ v]]
  (->> (proto/get-range key-store :vea (pack/pack-hash-array (pack/hash v)))
       (map (comp #(subvec % 1) pack/unpack-hash-array*))))

(defmethod get-from-index '[? :v ?]
  [key-store [_ a _]]
  (->> (proto/get-range key-store :aev (pack/pack-hash-array (pack/hash a)))
       (map (comp #(subvec % 1) pack/unpack-hash-array*))))

(defmethod get-from-index '[:v ? ?]
  [key-store [e _ _]]
  (->> (proto/get-range key-store :eav (pack/pack-hash-array (pack/hash e)))
       (map (comp #(subvec % 1) pack/unpack-hash-array*))))

(defmethod get-from-index '[? :v :v]
  [key-store [_ a v]]
  (->> (proto/get-range key-store :ave (pack/pack-hash-array (pack/hash a) (pack/hash v)))
       (map (comp #(subvec % 2) pack/unpack-hash-array*))))

(defmethod get-from-index '[:v ? :v]
  [key-store [e _ v]]
  (->> (proto/get-range key-store :eva (pack/pack-hash-array (pack/hash e) (pack/hash v)))
       (map (comp #(subvec % 2) pack/unpack-hash-array*))))

(defmethod get-from-index '[:v :v ?]
  [key-store [e a _]]
  (->> (proto/get-range key-store :eav (pack/pack-hash-array (pack/hash e) (pack/hash a)))
       (map (comp #(subvec % 2) pack/unpack-hash-array*))))

(defmethod get-from-index '[:v :v :v]
  [key-store [e a v]]
  (if (proto/get-k key-store :eav (pack/pack-hash-array (pack/hash e) (pack/hash a) (pack/hash v)))
    [[]]
    []))

(def ^:private fetch-limit 100)

(defn unpack-hash [^"[B" prefix-k ^"[B" k]
  #_(.getInt (ByteBuffer/wrap k) (count prefix-k))
  (ByteBuffer/wrap k (count prefix-k) pack/hash-length))

;; assumption is that (first cache) <= k <= (last cache)
(defn- binary-search
  [cache k]
  (loop [l 0 r (count cache)]
    (if (< l r)
      (let [m (quot (+ l r) 2)]
        (if (< (compare (nth cache m) k) 0)
          (recur (inc m) r)
          (recur l m)))
      (subvec cache l))))

(comment
  (binary-search [1 2 3] 2)
  (binary-search [1 7 8 9] 2)
  (binary-search [1 7 8 9] 0)
  (binary-search [1 7 8 9] 12))

;; cache is for now just a vector of previously fetched items
;; TODO think about how to deal with state (should we prefetch?)
;; TODO should the count be used to not fetch more
;; TODO if cache < fetch-limit remember it's finished
;; try to defer BufferWrap to key and binary-search
(defrecord RedisIterator [key-store keyspace prefix-k tuple cache]
  itr/LeapIterator
  (key [this] (first cache))
  (next [this]
    (assert (seq cache) "Cache can not be empty!")
    (if-let [new-cache (next cache)]
      (->RedisIterator key-store keyspace prefix-k tuple new-cache)
      ;; here k is a wrapped ByteBuffer
      ;; maybe copy can be avoided
      (let [new-cache (->> (proto/seek key-store keyspace (pack/inc-ba (pack/copy (.array (first cache)))) fetch-limit)
                           (map (partial unpack-hash prefix-k))
                           vec)]
        (if (seq new-cache)
          (->RedisIterator key-store keyspace prefix-k tuple new-cache)
          (->RedisIterator key-store keyspace prefix-k tuple nil)))))
  (seek [this k]
    (if (<= (compare k (last cache)) 0)
      (->RedisIterator key-store keyspace prefix-k tuple (binary-search cache k))
      (let [new-cache (->> (proto/get-range key-store keyspace
                                            (pack/concat-ba prefix-k (.array k))
                                            (pack/inc-ba (pack/copy prefix-k)) fetch-limit)
                           (map (partial unpack-hash prefix-k))
                           vec)]
        (if (seq new-cache)
          (->RedisIterator key-store keyspace prefix-k tuple new-cache)
          (->RedisIterator key-store keyspace prefix-k tuple nil)))))
  (at-end? [this] (empty? cache))
  itr/IteratorCount
  (count* [this]
    (proto/count-ks key-store keyspace prefix-k)))


;; TODO the hashing should be moved in here
;; or be replaced by byte array comparison
(defn ->redis-iterator [{:keys [triple-order triple] :as tuple} key-store]
  ;; FIX add spec for persistent connections
  (s/assert ::h-spec/persistent-tuple tuple)
  (let [keyspace (keyword (apply str (map name triple-order)))
        prefix-k (apply pack/pack-hash-array-bb (butlast triple))]
    (if-let [cache (seq (proto/seek key-store keyspace prefix-k fetch-limit))]
      (->RedisIterator key-store keyspace prefix-k tuple (vec (map (partial unpack-hash prefix-k) cache)))
      (->RedisIterator key-store keyspace prefix-k tuple nil))))
