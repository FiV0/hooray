(ns hooray.db.memory.bitemp-graph
  (:require [hooray.util :as util :refer [dissoc-in]]
            [hooray.datom :as datom]
            [hooray.graph :as graph]
            [com.dean.interval-tree.core :as dean]
            [java-time :as jt])
  (:import (java.time Instant)
           (java.util Date)))

(declare memory-bitemp-graph)
(declare get-from-index)
(declare transact)

(defrecord MemoryBitempGraph [teav tave tvea]
  graph/Graph
  (new-graph [this] (memory-bitemp-graph))
  (graph-add [this triple] (throw (ex-info "todo" {})))
  (graph-delete [this triple] (throw (ex-info "todo" {})))
  (graph-transact [this tx-id assertions retractions] (throw (ex-info "todo" {})))
  (resolve-triple [this triple] (get-from-index this triple))
  (transact [this tx-data ts] (transact this tx-data ts)))

(defn- interval-map [] (dean/interval-map {}))

(defn- now-date [] (java.util.Date.))
(defn- now-instant [] (jt/instant))

(jt/instant 0)

(jt/instant (/ Long/MAX_VALUE 10))

(java.util.Date 0)


(do #inst "2018-05-18T09:20:27.966-00:00")


(defn memory-bitemp-graph []
  (->MemoryBitempGraph (interval-map) (interval-map) (interval-map)))

(defn- map->triples [m valid-from valid-to]
  (let [eid (or (:db/id m) (random-uuid))]
    (->> (dissoc m :db/id)
         (map (fn [[k v]] (vector eid k v valid-from valid-to))))))


(defn add-valid-times [])

(defn transaction->triples [[type value valid-from valid-to]]
  (cond
    (map? value) (map->triples transaction ts)
    (= :db/add (first transaction)) [(vec (concat (rest transaction) [ts true]))]
    (= :db/retract (first transaction)) [(vec (concat (rest transaction) [ts false]))]))


;;(< (util/now) (do (Thread/sleep 100) (util/now)))

(def ^:private index-types [:teav :tave :tvea])

(defn triple-reorder-fn [index-type]
  (case index-type
    :teav identity
    :tave (fn [[e a v valid-from valid-to]] [a v e valid-from valid-to])
    :tvea (fn [[e a v valid-from valid-to]] [e a v valid-from valid-to])
    (throw (ex-info "No such index!" {}))))

(defn index-triple-add [index [v1 v2 v3 valid-from valid-to]]
  (update-in index [[valid-from valid-to] v1 v2] (fnil conj #{}) v3))

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

kkkk
