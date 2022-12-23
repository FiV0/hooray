(ns hooray.db.persistent.graph
  (:require [clojure.spec.alpha :as s]
            [hooray.db.persistent.packing :as pack]
            [hooray.db.persistent.protocols :as proto]
            [hooray.graph :as g]
            [hooray.query.spec :as h-spec]
            [hooray.util :as util]))

(declare get-from-index*)

(defrecord PersistentGraph [connection]
  g/Graph
  (new-graph [this] (util/unsupported-ex))
  (new-graph [this _opts] (util/unsupported-ex))
  (graph-add [this _triple] (util/unsupported-ex))
  (graph-delete [this _triple] (util/unsupported-ex))
  (graph-transact [this _tx-id _assertions _retractions] (util/unsupported-ex))
  (transact [this _tx-data] (util/unsupported-ex))
  (transact [this _tx-data _ts] (util/unsupported-ex))

  (resolve-triple [this triple] (get-from-index* (:key-store connection)
                                                 triple))
  (resolve-triple [this _triple _ts] (util/unsupported-ex))

  g/GraphIndex
  (resolve-tuple [this tuple]
    (s/assert ::h-spec/tuple tuple)
    (util/unsupported-ex))
  (get-iterator [this _tuple] (util/unsupported-ex))
  (get-iterator [this _tuple _type] (util/unsupported-ex)))


;; TODO optimize for not returning constant columns
(defn simplify [binding] (map #(if (util/variable? %) '? :v) binding))

(defmulti get-from-index (fn [key-store binding] (simplify binding)))

(defmethod get-from-index '[? ? ?]
  [key-store [_ _ _]]
  (proto/get-range key-store :eav))

(defmethod get-from-index '[? ? :v]
  [key-store [_ _ v]]
  (proto/seek key-store :vea (pack/pack-hash-array v)))

(defmethod get-from-index '[? :v ?]
  [key-store [_ a _]]
  (proto/seek key-store :aev (pack/pack-hash-array a)))

(defmethod get-from-index '[:v ? ?]
  [key-store [e _ _]]
  (proto/seek key-store :eav (pack/pack-hash-array e)))

(defmethod get-from-index '[? :v :v]
  [key-store [_ a v]]
  (proto/seek key-store :ave (pack/pack-hash-array a v)))

(defmethod get-from-index '[:v ? :v]
  [key-store [e _ v]]
  (proto/seek key-store :eva (pack/pack-hash-array e v)))

(defmethod get-from-index '[:v :v ?]
  [key-store [e a _]]
  (proto/seek key-store :eav (pack/pack-hash-array e a)))

(defmethod get-from-index '[:v :v :v]
  [key-store [e a v]]
  (if (proto/get-k key-store :eav (pack/pack-hash-array e a v))
    [[]]
    []))

(defn get-from-index* [key-store binding]
  (->> (get-from-index key-store binding)
       (map pack/unpack-hash-array)))
