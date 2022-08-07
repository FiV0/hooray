(ns hooray.db.memory.bitemp-graph
  (:require [clojure.core.rrb-vector :as fv]
            [com.dean.interval-tree.core :as dean]
            [hooray.datom :as datom]
            [hooray.graph :as graph]
            [hooray.util :as util :refer [dissoc-in]]
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
  (transact [this tx-data] (transact this tx-data)))

(defn- interval-map [] (dean/interval-map {}))

(defn- ->inst
  ([] (java.util.Date.))
  ([ms] (java.util.Date ms)))

(def ^:private min-ms 0)
(def ^:private max-ms Long/MAX_VALUE)

(defn- inst->ms [inst default]
  (if inst
    (inst-ms inst)
    default))

(defn- triple-vt->ms [[e a v vt-start vt-end]]
  [e a v (inst->ms vt-start min-ms) (inst->ms vt-end max-ms)])

(defn- triple-vt->inst [[e a v vt-start vt-end]]
  [e a v (->inst vt-start) (->inst vt-end)])

(defn memory-bitemp-graph []
  (->MemoryBitempGraph (interval-map) (interval-map) (interval-map)))

(defn- map->triples [m]
  (let [eid (or (:db/id m) (random-uuid))]
    (->> (dissoc m :db/id)
         (map (fn [[k v]] (vector eid k v))))))

(defn transaction->triples [[type value vt-start vt-end]]
  (let [vt-start (inst->ms vt-start min-ms)
        vt-end (inst->ms vt-end max-ms)]
    (if (map? value)
      (->> (map->triples value)
           (map #(into % [vt-start vt-end (= :db/add type)])))
      [(into value [vt-start vt-end (= :db/add type)])])))

(comment
  (def now (jt/instant))
  (transaction->triples [:db/add {:foo/bar 1 :bar/foo 2} now (jt/instant (+ (inst-ms now) 100000))])
  )


(def ^:private index-types [:teav :tave :tvea])

(defn triple-reorder-fn [index-type]
  (case index-type
    :teav identity
    :tave (fn [[e a v vt-start vt-end added]] [a v e vt-start vt-end added])
    :tvea (fn [[e a v vt-start vt-end added]] [e a v vt-start vt-end added])
    (throw (ex-info "No such index!" {}))))

(defn index-triple-add [index [v1 v2 v3 vt-start vt-end]]
  (let [interval [vt-start vt-end]
        old-val (some-> (find index interval) val)]
    (->>
     (update-in (if old-val old-val {}) [v1 v2] (fnil conj #{}) v3)
     (assoc index interval))))

(defn index-triple-retract [index [v1 v2 v3 vt-start vt-end]]
  (let [new-v3s (disj (get-in index [[vt-start vt-end] v1 v2]) v3)]
    (if (seq new-v3s)
      (assoc-in index [[vt-start vt-end] v1 v2] new-v3s)
      (dissoc-in index [[vt-start vt-end] v1 v2]))))

(defn index-triple [index triple]
  (if (nth triple 5)
    (index-triple-add index triple)
    (index-triple-retract index triple)))

(defn index-triples [index triples]
  (reduce index-triple index triples))

(defn insert-triples [graph triples]
  (reduce (fn [graph index-type]
            (update graph index-type index-triples
                    (map (triple-reorder-fn index-type) triples)))
          graph index-types))

(defn transact [graph tx-data]
  (insert-triples graph (mapcat transaction->triples tx-data)))

(comment
  (def now (jt/instant))
  (def later (jt/instant (+ (inst-ms now) 10000000000)))
  (def data [[:db/add {:foo/bar 1 :bar/foo 2} now later]
             [:db/add {:foo/too 1 :bar/toto 2} now later]])

  (->> (mapcat transaction->triples data)
       (map (triple-reorder-fn :tave)))

  (def g (transact (memory-bitemp-graph) data))

  )

(defn simplify [binding] (map #(if (util/variable? %) '? :v) binding))

(defmulti get-from-index (fn [indices binding ts] (simplify binding)))

(defmethod get-from-index '[? ? ?]
  [{index :teav} _ ts]
  (for [m (get index ts) e (keys m) a (keys (m e)) v ((m e) a)]
    [e a v]))

(defmethod get-from-index '[? ? :v]
  [{index :tvea} [_ _ v] ts]
  (for [m (get index ts) e (keys (m v)) a ((m v) e)]
    [e a v]))

(defmethod get-from-index '[? :v ?]
  [{index :tave} [_ a _] ts]
  (for [m (get index ts) v (keys (m a)) e ((m a) v)]
    [e a v]))

(defmethod get-from-index '[:v ? ?]
  [{index :teav} [e _ _] ts]
  (for [m (get index ts) a (keys (m e)) v ((m e) a)]
    [e a v]))

(defmethod get-from-index '[? :v :v]
  [{index :tave} [_ a v] ts]
  (for [e (get-in ()[a v])]
    [e a v]))

(defmethod get-from-index '[:v ? :v]
  [{index :vea} [e _ v]]
  (for [a (get-in index [v e])]
    [e a v]))

(defmethod get-from-index '[:v :v ?]
  [{index :vea} [e _ v]]
  (for [a (get-in index [v e])]
    [e a v]))

(defmethod get-from-index '[:v :v :v]
  [{index :eav} [e a v]]
  (if ((get-in index [e a]) v)
    [[e a v]]
    []))

(comment
  (get-from-index g '[?a ?b ?c] (inst-ms now))

  )
