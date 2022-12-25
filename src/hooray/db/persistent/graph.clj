(ns hooray.db.persistent.graph
  (:require [clojure.spec.alpha :as s]
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
