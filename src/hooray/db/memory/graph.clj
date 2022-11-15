(ns hooray.db.memory.graph
  (:require [clojure.tools.logging :as log]
            [hooray.datom :as datom]
            [hooray.graph :as graph]
            [hooray.util :as util :refer [dissoc-in]]))

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

  #_graph/GraphIndex
  #_(resolve-singleton [this type singleton] (get-from-index-unary this type singleton))
  #_(resolve-two-tuple [this type tuple] (get-from-index-unary this type tuple)))

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
  (let [triples (mapcat #(transaction->triples % ts) tx-data)]
    (insert-triples graph triples)))

(comment
  (def triples (->> (range 100)
                    (partition 5)
                    (map #(apply vector %))
                    (take 3)))

  (def g (insert-triples (memory-graph) triples))
  (transact (memory-graph) [{:foo/data 1}] (util/now))

  (get-from-index g '[?a ?b ?c])



  (def retraction (vector 0 1 2 nil false))

  (insert-triples g [retraction])

  )

;; TODO optimize for not returning constant columns
(defn simplify [binding] (map #(if (util/variable? %) '? :v) binding))

(defmulti get-from-index (fn [index binding] (simplify binding)))

(defmethod get-from-index '[? ? ?]
  [{:keys [eav vea ave]} [var1 var2 var3]]
  (cond (= var1 var2 var3)
        (for [e (keys eav)
              :when (get-in eav [e e e])]
          [e e e])

        (= var1 var2)
        (for [e (keys eav) v (get-in eav [e e])]
          [e e v])

        (= var1 var3)
        (for [v (keys vea) a (get-in vea [v v])]
          [v a v])

        (= var2 var3)
        (for [a (keys ave) e (get-in ave [a a])]
          [e a a])

        :else
        (for [e (keys eav) a (keys (eav e)) v ((eav e) a)]
          [e a v])))

(defmethod get-from-index '[? ? :v]
  [{index :vea} [var1 var2 v]]
  (if (= var1 var2)
    (for [e (keys (index v))
          :when (get-in index [v e e])]
      [e e])
    (for [e (keys (index v)) a ((index v) e)]
      [e a])))

(defmethod get-from-index '[? :v ?]
  [{index :ave} [var1 a var2]]
  (if (= var1 var2)
    (for [v (keys (index a))
          :when (get-in index [a v v])]
      [v v])
    (for [v (keys (index a)) e ((index a) v)]
      [e v])))

(defmethod get-from-index '[:v ? ?]
  [{index :eav} [e var1 var2]]
  (if (= var1 var2)
    (for [a (keys (index e))
          :when (get-in index [e a a])]
      [a a])
    (for [a (keys (index e)) v ((index e) a)]
      [a v])))

(defmethod get-from-index '[? :v :v]
  [{index :ave} [_ a v]]
  (for [e (get-in index [a v])]
    [e]))

(defmethod get-from-index '[:v ? :v]
  [{index :vea} [e _ v]]
  (for [a (get-in index [v e])]
    [a]))

(defmethod get-from-index '[:v :v ?]
  [{index :eav } [e a _]]
  (for [v (get-in index [e a])]
    [v]))

;; a nil v value is a problem
(defmethod get-from-index '[:v :v :v]
  [{index :eav} [e a v]]
  (if-let [v-index (get-in index [e a])]
    (if (v-index v)
      [[]]
      [])
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
