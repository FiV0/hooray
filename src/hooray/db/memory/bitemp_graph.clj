(ns hooray.db.memory.bitemp-graph
  (:require [hooray.util :as util :refer [dissoc-in]]
            [hooray.datom :as datom]
            [hooray.graph :as graph]))

(declare memory-bitemp-graph)
(declare get-from-index)

(defrecord MemoryBitempGraph [teav tave tvea]
  graph/Graph
  (new-graph [this] (memory-bitemp-graph))
  (graph-add [this triple] (throw (ex-info "todo" {})))
  (graph-delete [this triple] (throw (ex-info "todo" {})))
  (graph-transact [this tx-id assertions retractions] (throw (ex-info "todo" {})))
  (resolve-triple [this triple] (get-from-index this triple)))

(defn memory-bitemp-graph []
  (->MemoryBitempGraph (sorted-map) (sorted-map) (sorted-map)))

;;(< (util/now) (do (Thread/sleep 100) (util/now)))

(def ^:private index-types [:teav :tave :tvea])

(defn triple-reorder-fn [index-type]
  (case index-type
    :teav identity
    :tave (fn [[e a v valid-from valid-to]] [a v e valid-from valid-to])
    :tvea (fn [[e a v valid-from valid-to]] [e a v valid-from valid-to])
    (throw (ex-info "No such index!" {}))))

(defn index-triple-add [index [v1 v2 v3 valid-from valid-to]]
  (-> index
      (update-in [valid-from v1 v2 v3] (fnil conj #{}) true)
      (update-in [valid-to v1 v2 v3] (fnil conj #{}) false)))

#_(defn index-triple-retract [index [v1 v2 v3]]
    (let [new-v3s (disj (get-in index [v1 v2]) v3)]
      (if (seq new-v3s)
        (assoc-in index [v1 v2] new-v3s)
        (dissoc-in index [v1 v2]))))

(defn index-triple [index triple]
  (index-triple-add index triple))

(defn index-triples [index triples]
  (reduce index-triple index triples))

(defn insert-triples [graph triples]
  (reduce (fn [graph index-type]
            (update graph index-type index-triples
                    (map (triple-reorder-fn index-type) triples)))
          graph index-types))

;; (defn in)
