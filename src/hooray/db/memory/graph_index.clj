(ns hooray.db.memory.graph-index
  (:refer-clojure :exclude [hash key next])
  (:require [clojure.set]
            [clojure.spec.alpha :as s]
            [clojure.data.avl :as avl]
            [hooray.datom :as datom]
            [hooray.graph :as graph]
            [hooray.query.spec :as h-spec]
            [hooray.util :as util :refer [dissoc-in]]))

(s/def ::tuple (s/and (s/keys :req-un [::h-spec/triple ::h-spec/triple-order])
                      (fn [{:keys [triple triple-order]}] (= (count triple) (count triple-order)))))

(comment
  (s/valid? ::tuple {:triple '[?e :foo/bar]
                     :triple-order '[:e :a]})

  (def tuple (s/conform ::tuple {:triple '[?e :foo/bar]
                                 :triple-order '[:e :a]}) )




  )

;; TODO
;; add avl version
;; add transients during construction

(defn hash [v] (clojure.core/hash v))

(declare memory-graph)
(declare get-from-index)
(declare transact)
(declare get-from-index)
(declare get-iterator*)

(def ^:private iterator-types #{:simple :core :avl})

(defrecord MemoryGraphIndexed [eav eva ave aev vea vae doc-store opts]
  graph/Graph
  (new-graph [this] (memory-graph {:type :core}))
  (new-graph [this opts] (memory-graph opts))

  (graph-add [this triple] (throw (ex-info "todo" {})))
  (graph-delete [this triple] (throw (ex-info "todo" {})))
  (graph-transact [this tx-id assertions retractions] (throw (ex-info "todo" {})))
  (resolve-triple [this triple] (throw (ex-info "todo" {})) #_(get-from-index this triple))

  (transact [this tx-data] (transact this tx-data (util/now)))
  (transact [this tx-data ts] (transact this tx-data ts))

  graph/GraphIndex
  (resolve-tuple [this tuple]
    (s/assert ::tuple tuple)
    (get-from-index this tuple))

  (get-iterator
    [this tuple] (graph/get-iterator this tuple :simple)
    [this tuple type]
    )
  )

(defn sorted-set* [type]
  (case type
    :core (sorted-set)
    :avl (avl/sorted-set)
    (throw (ex-info "No such sorted-set type!" {:type type}))))

(defn sorted-map* [type]
  (case type
    :core (sorted-map)
    :avl (avl/sorted-map)
    (throw (ex-info "No such sorted-map type!" {:type type}))))

(defn memory-graph [{:keys [type] :as opts}]
  (->MemoryGraphIndexed (sorted-map* type) (sorted-map* type) (sorted-map* type)
                        (sorted-map* type) (sorted-map* type) (sorted-map* type)
                        {} opts))

;; TODO maybe assert :db/id
(defn- map->triples [m ts]
  (let [eid (or (:db/id m) (random-uuid))]
    (->> (dissoc m :db/id)
         (map (fn [[k v]] (vector eid k v ts true))))))

(defn transaction->triples [transaction ts]
  (cond
    (map? transaction) (map->triples transaction ts)
    (= :db/add (first transaction)) [(vec (concat (rest transaction) [ts true]))]
    (= :db/retract (first transaction)) [(vec (concat (rest transaction) [ts false]))]))

(def ^:private index-types #{:ea :ae :ev :ve :av :va})

(defn ->hash-triple [triple]
  (mapv hash (take 3 triple)))

(defn <-hash-triple [{:keys [doc-store] :as _graph} triple]
  (mapv #(get doc-store %) (take 3 triple)))

(defn index-triple-add [{opts :opts :as graph} [e a v :as triple]]
  (let [type (:type opts)
        [he ha hv] (->hash-triple triple)]
    (-> graph
        (update-in [:eav he ha] (fnil conj (sorted-set* type)) hv)
        (update-in [:eva he hv] (fnil conj (sorted-set* type)) ha)
        (update-in [:ave ha hv] (fnil conj (sorted-set* type)) he)
        (update-in [:aev ha he] (fnil conj (sorted-set* type)) hv)
        (update-in [:vea hv he] (fnil conj (sorted-set* type)) ha)
        (update-in [:vae hv ha] (fnil conj (sorted-set* type)) he)
        (update :doc-store into [[he e] [ha a] [hv v]]))))

(defn- retract-from-index [graph index [h1 h2 h3]]
  (let [new-h3s (disj (get-in graph [index h1 h2]) h3)]
    (if (seq new-h3s)
      (assoc-in graph [index h1 h2] new-h3s)
      (dissoc-in graph [index h1 h2]))))

(defn index-triple-retract [graph triple]
  (let [[he ha hv] (->hash-triple triple)]
    (-> graph
        (retract-from-index :eav [he ha hv])
        (retract-from-index :eva [he hv ha])
        (retract-from-index :ave [ha hv he])
        (retract-from-index :aev [ha he hv])
        (retract-from-index :vea [hv he ha])
        (retract-from-index :vae [hv ha he])
        (update :doc-store dissoc he ha hv))))

(defn index-triple [graph [_ _ _ _ added :as triple]]
  (if added
    (index-triple-add graph triple)
    (index-triple-retract graph triple)))

(defn index-triples [graph triples]

  (reduce index-triple graph triples))

(defn entity [{:keys [eav] :as graph} eid]
  (-> (get eav eid)
      (update-vals first)))

(defn transact [graph tx-data ts]
  (let [triples (mapcat #(transaction->triples % ts) tx-data)]
    (index-triples graph triples)))

(comment
  (def triples (->> (range 100)
                    (partition 5)
                    (map #(apply vector %))
                    (take 3)))

  (def g (index-triples (memory-graph {:type :core}) triples))
  (transact (memory-graph {:type :core}) [{:foo/data 1}] (util/now))

  (def retraction (vector 0 1 2 nil false))

  (index-triples g [retraction])

  )

;; pretty much one to one copied from asami
;; TODO optimize for not returning constant columns
(defn simplify [binding] (map #(if (util/variable? %) '? :v) binding))

(defmulti get-from-index (fn [_graph {:keys [triple] :as _tuple}] (simplify triple)))

(defmethod get-from-index :default [_ tuple]
  (throw (ex-info "No method found for tuple!" {:tuple tuple})))

(defmethod get-from-index '[? ? ?]
  [graph {[t1 t2 t3] :triple-order :as _tuple}]
  (let [index (get graph (keyword (str (name t1) (name t2) (name t3))))]
    (for [v1 (keys index) v2 (keys (index v1)) v3 (get-in index [v1 v2])]
      [v1 v2 v3])))

(defmethod get-from-index '[? ? :v]
  [graph {[t1 t2 t3] :triple-order [_ _ v3] :triple :as _tuple}]
  (let [index (get graph (keyword (str (name t3) (name t1) (name t2))))]
    (for [v1 (keys (index v3)) v2 (get-in index [v3 v1])]
      [v1 v2])))

(defmethod get-from-index '[? :v ?]
  [graph {[t1 t2 t3] :triple-order [_ v2 _] :triple :as _tuple}]
  (let [index (get graph (keyword (str (name t2) (name t1) (name t3))))]
    (for [v1 (keys (index v2)) v3 (get-in index [v2 v1])]
      (do (println v1 v3)
          [v1 v3]))))

(defmethod get-from-index '[:v ? ?]
  [graph {[t1 t2 t3] :triple-order [v1 _ _] :triple :as _tuple}]
  (let [index (get graph (keyword (str (name t1) (name t2) (name t3))))]
    (for [v2 (keys (index v1)) v3 (get-in index [v1 v2])]
      [v2 v3])))

(defmethod get-from-index '[? :v :v]
  [graph {[t1 t2 t3] :triple-order [_ v2 v3] :triple :as _tuple}]
  (let [index (get graph (keyword (str (name t2) (name t3) (name t1))))]
    (for [v1 (get-in index [v2 v3])]
      [v1])))

(defmethod get-from-index '[:v ? :v]
  [graph {[t1 t2 t3] :triple-order [v1 _ v3] :triple :as _tuple}]
  (let [index (get graph (keyword (str (name t1) (name t3) (name t2))))]
    (for [v2 (get-in index [v1 v3])]
      [v2])))

(defmethod get-from-index '[:v :v ?]
  [graph {[t1 t2 t3] :triple-order [v1 v2 _] :triple :as _tuple}]
  (let [index (get graph (keyword (str (name t1) (name t2) (name t3))))]
    (for [v3 (get-in index [v1 v2])]
      [v3])))

;; a nil v value is a problem
(defmethod get-from-index '[:v :v :v]
  [graph {[t1 t2 t3] :triple-order [v1 v2 v3] :triple :as _tuple}]
  (let [index (get graph (keyword (str (name t1) (name t2) (name t3))))]
    (if ((get-in index [v1 v2]) v3)
      [[]]
      [])))

(defn- missing [types]
  (clojure.set/difference #{:e :a :v} types))

(comment
  (missing #{:e }))

(defmethod get-from-index '[? ?]
  [graph {[t1 t2] :triple-order :as _tuple}]
  (let [index (get graph (keyword (str (name t1) (name t2) (name (first (missing #{t1 t2}))))))]
    (for [v1 (keys index) v2 (keys (index v1))]
      [v1 v2])))

(defmethod get-from-index '[? :v]
  [graph {[t1 t2] :triple-order [_ v2] :triple :as _tuple}]
  (let [index (get graph (keyword (str (name t2) (name t1) (name (first (missing #{t1 t2}))))))]
    (for [v1 (keys (index v2))]
      [v1])))

(defmethod get-from-index '[:v ?]
  [graph {[t1 t2] :triple-order [v1 _] :triple :as _tuple}]
  (let [index (get graph (keyword (str (name t1) (name t2) (name (first (missing #{t1 t2}))))))]
    (for [v2 (keys (index v1))]
      [v2])))

(defmethod get-from-index '[:v :v]
  [graph {[t1 t2] :triple-order [v1 v2] :triple :as _tuple}]
  (let [index (get graph (keyword (str (name t1) (name t2) (name (first (missing #{t1 t2}))))))]
    (if (seq (get-in index [v1 v2]))
      [[]]
      [])))

(defmethod get-from-index '[?]
  [graph {[t1] :triple-order :as _tuple}]
  (let [missing (missing #{t1})
        index (get graph (keyword (str (name t1) (name (first missing)) (name (second missing)))))]
    (for [v1 (keys index)]
      [v1])))

(defmethod get-from-index '[:v]
  [graph {[t1] :triple-order [v1] :triple :as _tuple}]
  (let [missing (missing #{t1})
        index (get graph (keyword (str (name t1) (name (first missing)) (name (second missing)))))]
    (if (seq (get index v1))
      [[]]
      [])))

(comment
  (def g (transact (memory-graph {:type :core}) [{:type :the-first :data 2} {:type :the-second :data 3}] (util/now)))


  (get-from-index g {:triple ['?e (hash :type) '?t]
                     :triple-order [:e :a :v]})

  (get-from-index g {:triple [(hash #uuid "3a3e3530-73f1-4e63-8379-7e1e3ab0e664") (hash :type) '?t]
                     :triple-order [:e :a :v]})


  (get-from-index g {:triple [(hash :type) '?t]
                     :triple-order [:a :v]})

  (get-from-index g {:triple ['?e]
                     :triple-order [:e]})


  (require 'hooray.db.memory.graph-index :reload)

  )

(defmulti get-index (fn [graph {:keys [triple] :as _tuple}] (simplify triple)))

(defmethod get-index :default [_ tuple]
  (throw (ex-info "No method found for tuple!" {:tuple tuple})))

(defn triple-order->index [[t1 t2 t3]])

(defmethod get-index '[? ? ?]
  [graph {[t1 t2 t3] :triple-order :as _tuple}]
  (get graph (keyword (str (name t1) (name t2) (name t3)))))

(defmethod get-index '[? ? :v]
  [graph {[t1 t2 t3] :triple-order [_ _ v3] :triple :as _tuple}]
  (get-in graph [(keyword (str (name t3) (name t1) (name t2))) v3]))

(defmethod get-index '[? :v ?]
  [graph {[t1 t2 t3] :triple-order [_ v2 _] :triple :as _tuple}]
  (get-in graph [(keyword (str (name t2) (name t1) (name t3))) v2]))

(defmethod get-index '[:v ? ?]
  [graph {[t1 t2 t3] :triple-order [v1 _ _] :triple :as _tuple}]
  (get-in graph [(keyword (str (name t1) (name t2) (name t3))) v1]))

(defmethod get-index '[? :v :v]
  [graph {[t1 t2 t3] :triple-order [_ v2 v3] :triple :as _tuple}]
  (get-in graph [(keyword (str (name t2) (name t3) (name t1))) v2 v3]))

(defmethod get-index '[:v ? :v]
  [graph {[t1 t2 t3] :triple-order [v1 _ v3] :triple :as _tuple}]
  (get-in graph [(keyword (str (name t1) (name t3) (name t2))) v1 v3]))

(defmethod get-index '[:v :v ?]
  [graph {[t1 t2 t3] :triple-order [v1 v2 _] :triple :as _tuple}]
  (get-in graph [(keyword (str (name t1) (name t2) (name t3))) v1 v2]))

;; a nil v value is a problem
(defmethod get-index '[:v :v :v]
  [graph {[t1 t2 t3] :triple-order [v1 v2 v3] :triple :as _tuple}]
  (throw (ex-info "todo" {})))

;; FIXME maybe do a stateful and non-stateful version
;; TODO think about if next should go one level up

(defprotocol LeapIterator
  (key [this])
  (next [this])
  (seek [this k])
  (at-end? [this]))

(defprotocol LeapLevels
  (open [this])
  (up [this]))

(defrecord SimpleIterator [data prefix depth max-depth]
  LeapIterator
  (key [this] (nth (first data) depth))

  (next [this]
    (->SimpleIterator (rest data) prefix depth max-depth))

  (seek [this k]
    (let [kk (conj prefix k)]
      (->SimpleIterator (drop-while #(<= (compare % kk) -1) data) prefix depth max-depth)))

  (at-end? [this] (or (empty? data) (<= (compare prefix (take (count prefix) (first data))) -1)))

  LeapLevels
  (open [this]
    (assert (< (inc depth) max-depth))
    (->SimpleIterator data (conj prefix (nth (first data) depth)) (inc depth) max-depth))

  (up [this]
    (assert (> depth 0))
    (->SimpleIterator data (pop prefix) (dec depth) max-depth)))

(defn ->simple-iterator [data]
  (->SimpleIterator data [] 0 (count (first data))))

(defn tuple->simple-iterator [graph tuple]
  (->simple-iterator (get-from-index graph tuple)))

(defn- first-key [index depth max-depth]
  (if (= depth max-depth)
    (first index)
    (ffirst index)))

(defn- seek-key [index k depth max-depth]
  (let [key-fn (if (= depth max-depth) identity first)]
    (loop [l 0 h (dec (count index))]
      (if (= l h)
        (if (<= k (key-fn (nth index l)))
          (subvec index l (count index))
          [])
        (let [m (quot (+ l h) 2)]
          (if (<= k (key-fn (nth index m)))
            (recur l m)
            (recur (inc m) h)))))))

(comment
  (seek-key [1 2 3 4 9 10 12] 3 0 0)
  (seek-key [1 2 3 4 9 10 12] 13 0 0)
  (seek-key [1] 13 0 0)
  (seek-key [0 1] 1 0 0))

(defrecord LeapIteratorCore [index stack depth max-depth]
  LeapIterator
  (key [this] (first-key index depth max-depth))

  (next [this] (->LeapIteratorCore (clojure.core/next index) stack depth max-depth))

  (seek [this k]
    (when (seq index)
      (->LeapIteratorCore (seek-key index k depth max-depth) stack depth max-depth)))

  (at-end? [this] (empty? index))

  LeapLevels
  (open [this]
    (assert (< (inc depth) max-depth))
    (->LeapIteratorCore (-> index first second vec) (conj stack index) (inc depth) max-depth))

  (up [this]
    (assert (> depth 0))
    (->LeapIteratorCore (peek index) (pop stack) (dec depth) max-depth)))

(defn ->leap-iterator-core [index max-depth]
  (->LeapIteratorCore (vec index) [] 0 max-depth))

(defrecord LeapIteratorAVL [index stack depth max-depth]
  LeapIterator
  (key [this] (first-key index depth max-depth))

  (next [this] (->LeapIteratorCore (clojure.core/next index) stack depth max-depth))

  (seek [this k]
    (when (seq index)
      (->LeapIteratorCore (avl/seek index k) stack depth max-depth)))

  (at-end? [this] (empty? index))

  LeapLevels
  (open [this]
    (assert (< (inc depth) max-depth))
    (->LeapIteratorCore (-> index first second seq) (conj stack index) (inc depth) max-depth))

  (up [this]
    (assert (> depth 0))
    (->LeapIteratorCore (peek index) (pop stack) (dec depth) max-depth)))

(defn- avl-index? [index]
  (or (instance? clojure.data.avl.AVLMap index)
      (instance? clojure.data.avl.AVLSet index)))

(defn ->leap-iterator-avl [index max-depth]
  {:pre [(assert (avl-index? index))]}
  (->LeapIteratorCore (seq index) [] 0 max-depth))

(defn get-iterator* [graph {:keys [triple] :as tuple} type]
  {:pre [(s/assert ::tuple tuple) (assert (iterator-types type))]}
  (case type
    :simple (->simple-iterator (get-from-index graph tuple))
    :core (->leap-iterator-core (get-index graph tuple) (count (h-spec/triple->logic-vars triple)))
    :avl (->leap-iterator-avl (get-index graph tuple) (count (h-spec/triple->logic-vars triple)))
    (throw (ex-info "todo" {}))))
