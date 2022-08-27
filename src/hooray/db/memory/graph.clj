(ns hooray.db.memory.graph
  (:require [hooray.util :as util :refer [dissoc-in]]
            [hooray.datom :as datom]
            [hooray.graph :as graph]))

;; TODO remove the datoms part
;; on this level we should not be concerned with datoms

(declare memory-graph)
(declare get-from-index)
(declare get-from-index-unary)
(declare get-from-index-binary)
(declare transact)

(defrecord MemoryGraph [eav ave vea]
  graph/Graph
  (new-graph [this] (memory-graph))
  (graph-add [this triple] (throw (ex-info "todo" {})))
  (graph-delete [this triple] (throw (ex-info "todo" {})))
  (graph-transact [this tx-id assertions retractions] (throw (ex-info "todo" {})))
  (resolve-triple [this triple] (get-from-index this triple))
  (transact [this tx-data ts] (transact this tx-data ts))

  graph/GraphIndex
  (resolve-singleton [this type singleton] (get-from-index-unary this type singleton))
  (resolve-two-tuple [this type tuple] (get-from-index-unary this type tuple)))

(defn memory-graph []
  (->MemoryGraph {} {} {}))

(defn- map->triples [m ts]
  (let [eid (or (:db/id m) (random-uuid))]
    (->> (dissoc m :db/id)
         (map (fn [[k v]] (vector eid k v ts true))))))

(defn transaction->triples [transaction ts]
  (cond
    (map? transaction) (map->triples transaction ts)
    (= :db/add (first transaction)) [(vec (concat (rest transaction) [ts true]))]
    (= :db/retract (first transaction)) [(vec (concat (rest transaction) [ts false]))]))

(def ^:private index-types [:eav :ave :vea])

(defn triple-reorder-fn [index-type]
  (case index-type
    :eav identity
    :ave (fn [[e a v tx added]] [a v e tx added])
    :vea (fn [[e a v tx added]] [v e a tx added])
    (throw (ex-info "No such index!" {}))))

(defn index-triple-add [index [v1 v2 v3]]
  (update-in index [v1 v2] (fnil conj #{}) v3))

(defn index-triple-retract [index [v1 v2 v3]]
  (let [new-v3s (disj (get-in index [v1 v2]) v3)]
    (if (seq new-v3s)
      (assoc-in index [v1 v2] new-v3s)
      (dissoc-in index [v1 v2]))))

(defn index-triple [index [_ _ _ _ added :as triple]]
  (if added
    (index-triple-add index triple)
    (index-triple-retract index triple)))

(defn index-triples [index triples]
  (reduce index-triple index triples))

(defn insert-triples [graph triples]
  (reduce (fn [graph index-type]
            (update graph index-type index-triples
                    (map (triple-reorder-fn index-type) triples)))
          graph index-types))

(defn entity [{:keys [eav] :as graph} eid]
  (-> (get eav eid)
      (update-vals first)))

(defn transact [graph tx-data ts]
  (let [triples (map #(transaction->triples % ts) tx-data)]
    (insert-triples graph triples)))

(comment
  (def triples (->> (range 100)
                    (partition 5)
                    (map #(apply vector %))
                    (take 3)))

  (def g (insert-triples (memory-graph) triples))

  (def retraction (vector 0 1 2 nil false))

  (insert-triples g [retraction])

  )

;; pretty much one to one copied from asami
;; TODO optimize for not returning constant columns
(defn simplify [binding] (map #(if (util/variable? %) '? :v) binding))

(defmulti get-from-index (fn [index binding] (simplify binding)))

(defmethod get-from-index '[? ? ?]
  [{index :eav} _]
  (for [e (keys index) a (keys (index e)) v ((index e) a)]
    [e a v]))

(defmethod get-from-index '[? ? :v]
  [{index :vea} [_ _ v]]
  (for [e (keys (index v)) a ((index v) e)]
    [e a v]))

(defmethod get-from-index '[? :v ?]
  [{index :ave} [_ a _]]
  (for [v (keys (index a)) e ((index a) v)]
    [e a v]))

(defmethod get-from-index '[:v ? ?]
  [{index :eav} [e _ _]]
  (for [a (keys (index e)) v ((index e) a)]
    [e a v]))

(defmethod get-from-index '[? :v :v]
  [{index :ave} [_ a v]]
  (for [e (get-in index [a v])]
    [e a v]))

(defmethod get-from-index '[:v ? :v]
  [{index :vea} [e _ v]]
  (for [a (get-in index [v e])]
    [e a v]))

(defmethod get-from-index '[:v :v ?]
  [{index :eav } [e a _]]
  (for [v (get-in index [e a])]
    [e a v]))

;; a nil v value is a problem
(defmethod get-from-index '[:v :v :v]
  [{index :eav} [e a v]]
  (if ((get-in index [e a]) v)
    [[e a v]]
    []))

(def ^:private binary-index-remap
  {:ea :eav
   :av :ave
   :ve :vea})

(defmulti get-from-index-binary (fn [index type binding] (simplify binding)))

(defmethod get-from-index-binary '[? ?]
  [index type _]
  (let [index ((binary-index-remap type) index)]
    (for [v1 (keys index) v2 (keys (index v1))] [v1 v2])))

(defmethod get-from-index-binary '[:v ?]
  [index type [v1]]
  (let [index ((binary-index-remap type) index)]
    (for [v2 (keys (index v1))] [v1 v2])))

(defmethod get-from-index-binary '[? :v]
  [index type [_ v2]]
  (let [index ((binary-index-remap type) index)]
    (for [v1 (keys index) :when (get-in index [v1 v2])] [v1 v2])))

(defmethod get-from-index-binary '[:v :v]
  [index type [v1 v2]]
  (let [index ((binary-index-remap type) index)]
    (if (get-in index [v1 v2])
      [[v1 v2]]
      [])))

(def ^:private unary-index-remap
  {:e :eav
   :a :ave
   :v :vea})

(defmulti get-from-index-unary (fn [index type binding] (simplify binding)))

(defmethod get-from-index-unary '[?]
  [index type _]
  (let [index ((unary-index-remap type) index)]
    (for [v1 (keys index)] [v1])))

(defmethod get-from-index-unary '[:v]
  [index type [v1]]
  (let [index ((unary-index-remap type) index)]
    (if (get index v1)
      [[v1]]
      [])))
