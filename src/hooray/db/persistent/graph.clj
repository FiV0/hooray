(ns hooray.db.persistent.graph
  (:require [clojure.spec.alpha :as s]
            [hooray.db.iterator :as itr]
            [hooray.db.persistent.packing :as pack]
            [hooray.db.persistent.protocols :as proto]
            [hooray.graph :as g]
            [hooray.query.spec :as h-spec]
            [hooray.util :as util]))

(declare get-from-index)

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
  (get-iterator [this _tuple] (util/unsupported-ex))
  (get-iterator [this _tuple _type] (util/unsupported-ex)))

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

(def ^:private fetch-limit 10)

(defn- unpack-hash [^"[B" prefix-k ^"[B" k]
  (.getInt k (count prefix-k)))

;; assumption is that (first cache) <= k <= (last cache)
(defn- binary-search
  [cache k]
  (loop [l 0 r (count cache)]
    (if (< l r)
      (let [m (quot (+ l r) 2)]
        (if (< (nth cache m) k)
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
(defrecord RedisIterator [key-store keyspace prefix-k tuple cache]
  itr/LeapIterator
  (key [this] (first cache))
  (next [this]
    (assert (seq cache) "Cache can not be empty!")
    (if-let [cache (next cache)]
      (->RedisIterator key-store keyspace prefix-k tuple cache)
      (let [new-cache (vec (proto/seek key-store keyspace (pack/concat-ba prefix-k (pack/int->bytes (first cache))) fetch-limit))]
        (if (seq new-cache)
          (->RedisIterator key-store keyspace prefix-k tuple new-cache)
          (->RedisIterator key-store keyspace prefix-k tuple nil)))))
  (seek [this k]
    (if (<= k (last cache))
      (->RedisIterator key-store keyspace prefix-k tuple (binary-search cache k))
      (let [new-cache (vec (proto/seek key-store keyspace (pack/concat-ba prefix-k (pack/int->bytes k)) fetch-limit))]
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
  (let [keyspace (keyword (apply str (map name triple-order)))
        prefix-k (apply pack/pack-hash-array (butlast triple))]
    (if-let [cache (seq (proto/seek key-store keyspace prefix-k fetch-limit))]
      (->RedisIterator key-store keyspace prefix-k tuple (vec cache))
      (->RedisIterator key-store keyspace prefix-k tuple nil))))
