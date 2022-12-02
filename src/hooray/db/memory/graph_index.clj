(ns hooray.db.memory.graph-index
  (:refer-clojure :exclude [hash key next])
  (:require [clojure.data.avl :as avl]
            [clojure.set]
            [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [hooray.datom :as datom]
            [hooray.util.avl :as avl-util]
            [hooray.graph :as graph]
            [hooray.query.spec :as h-spec]
            [hooray.util :as util :refer [dissoc-in]]
            [hooray.util.persistent-map :as tonsky-map]
            [me.tonsky.persistent-sorted-set :as tonsky-set])
  (:import (me.tonsky.persistent_sorted_set Seq)))

(s/def ::tuple (s/and (s/keys :req-un [::h-spec/triple ::h-spec/triple-order])
                      (fn [{:keys [triple triple-order]}] (= (count triple) (count triple-order)))))

(comment
  (s/valid? ::tuple {:triple '[?e :foo/bar]
                     :triple-order '[:e :a]})

  (def tuple (s/conform ::tuple {:triple '[?e :foo/bar]
                                 :triple-order '[:e :a]})))

;; TODO
;; add transients during construction
;; Think about how to deal with duplicate vars in the iterators. The problem seems to be that
;; the triejoin algo then needs to take care of different levels for the same TrieIterator.

(defn hash [v] (clojure.core/hash v))

(declare memory-graph)
(declare get-from-index)
(declare transact)
(declare get-index)
(declare get-from-index)
(declare get-iterator*)

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
    (get-index this tuple))

  (get-iterator [this tuple] (get-iterator* this tuple :simple))
  (get-iterator [this tuple type] (get-iterator* this tuple type)))

(defmethod print-method MemoryGraphIndexed [_g ^java.io.Writer w]
  (.write w "MemoryGraphIndexed{}"))

(defn sorted-set* [type]
  (case type
    (:simple :core) (sorted-set)
    :avl (avl/sorted-set)
    :tonsky (tonsky-set/sorted-set)
    (throw (ex-info "No such sorted-set type!" {:type type}))))

(defn sorted-map* [type]
  (case type
    :core (sorted-map)
    :avl (avl/sorted-map)
    :tonsky (tonsky-map/sorted-map)
    (throw (ex-info "No such sorted-map type!" {:type type}))))

(defn memory-graph
  ([] (memory-graph {:type :core}))
  ([{:keys [type] :as opts}]
   (->MemoryGraphIndexed (sorted-map* type) (sorted-map* type) (sorted-map* type)
                         (sorted-map* type) (sorted-map* type) (sorted-map* type)
                         {} opts)))

(comment
  (graph/get-iterator (memory-graph {:type :core}) (s/conform ::tuple {:triple '[?e ?a ?v]
                                                                       :triple-order '[:e :a :v]})))

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

(def ^:private index-types #{:eav :eva :ave :aev :vea :vae})

(defn ->hash-triple [triple]
  (mapv hash (take 3 triple)))

(defn <-hash-triple [{:keys [doc-store] :as _graph} triple]
  (mapv #(get doc-store %) (take 3 triple)))

(defn value->hash [{:keys [_doc-store] :as _graph} value]
  (hash value))

(defn hash->value [{:keys [doc-store] :as _graph} h]
  (get doc-store h))

(defn index-triple-add [{opts :opts :as graph} [e a v :as triple]]
  (let [type (:type opts)
        update-in (case type
                    (:simple :core) (avl-util/create-update-in sorted-map)
                    :avl (avl-util/create-update-in avl/sorted-map)
                    :tonsky (avl-util/create-update-in tonsky-map/sorted-map)
                    (throw (ex-info "Unknown graph type!" {:type type})))
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
      [v1 v3])))

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
                     :triple-order [:e]}))

(defmulti get-index (fn [graph {:keys [triple] :as _tuple}] (simplify triple)))

(defmethod get-index :default [_ tuple]
  (throw (ex-info "No method found for tuple!" {:tuple tuple})))

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

(defmethod get-index '[? ?]
  [graph {[t1 t2] :triple-order :as _tuple}]
  (get graph (keyword (str (name t1) (name t2)) (name (first (missing #{t1 t2}))))))

(defmethod get-index '[? :v]
  [graph {[t1 t2] :triple-order [_ v2] :triple :as _tuple}]
  (get-in graph [(keyword (str (name t2) (name t1) (name (first (missing #{t1 t2}))))) v2]))

(defmethod get-index '[:v ?]
  [graph {[t1 t2] :triple-order [v1 _] :triple :as _tuple}]
  (get-in graph [(keyword (str (name t1) (name t2) (name (first (missing #{t1 t2}))))) v1]))

(defmethod get-index '[:v :v]
  [graph {[t1 t2] :triple-order [v1 v2] :triple :as _tuple}]
  (throw (ex-info "todo" {})))

(defmethod get-index '[?]
  [graph {[t1] :triple-order :as _tuple}]
  (let [missing (missing #{t1})]
    (get graph (keyword (str (name t1) (name (first missing)) (name (second missing)))))))

(defmethod get-from-index '[:v]
  [graph {[t1] :triple-order [v1] :triple :as _tuple}]
  (throw (ex-info "todo" {})))


;; FIXME maybe do a stateful and non-stateful version
;; TODO think about if next should go one level up

(defprotocol LeapIterator
  (key [this])
  (next [this])
  (seek [this k])
  (at-end? [this]))

(s/def :leap/iterator #(satisfies? LeapIterator %))

(defprotocol LeapLevels
  (open [this])
  (up [this])
  (level [this])
  (depth [this]))

(s/def :leap/levels #(satisfies? LeapLevels %))

(defn leap-iterator? [itr]
  (and (satisfies? LeapIterator itr) (satisfies? LeapLevels itr)))

(defn pop-empty [v]
  (if (seq v) (pop v) nil))

(defrecord SimpleIterator [data prefix depth max-depth end?]
  LeapIterator
  (key [this]
    (when-not end?
      (nth (first data) depth)))

  (next [this]
    (with-meta
      (if (and (not end?) (or (empty? (rest data)) (= prefix (subvec (second data) 0 (count prefix)))))
        (->SimpleIterator (rest data) prefix depth max-depth false)
        (->SimpleIterator data prefix depth max-depth true))
      (meta this)))

  (seek [this k]
    (let [kk (conj prefix k)
          data (drop-while #(<= (compare (subvec % 0 (count kk)) kk) -1) data)]
      (with-meta
        (->SimpleIterator data prefix depth max-depth
                          (or (empty? data) (<= (compare prefix (subvec (first data) 0 (count prefix))) -1)))
        (meta this))))

  (at-end? [this]
    #_(or (empty? (rest data)) (<= (compare prefix (subvec (second data) 0 (count prefix))) -1))
    (or end? (empty? data) (<= (compare prefix (subvec (first data) 0 (count prefix))) -1)))

  LeapLevels
  (open [this]
    (assert (< (inc depth) max-depth))
    (->SimpleIterator data (conj prefix (nth (first data) depth)) (inc depth) max-depth false))

  (up [this]
    #_(assert (> depth 0))
    #_(->SimpleIterator data (pop prefix) (dec depth) max-depth)
    (if (= depth 0)
      ((-> this meta :original-itr) )
      (with-meta (->SimpleIterator data (pop-empty prefix) (dec depth) max-depth false)
        (meta this))))

  (level [this] depth)

  (depth [this] max-depth))

(defmethod print-method SimpleIterator [_g ^java.io.Writer w]
  (.write w "#SimpleIterator{}"))

(defn ->simple-iterator [data]
  (let [simple-itr (->SimpleIterator data [] 0 (count (first data)) false)]
    (with-meta simple-itr {:original-itr #(->simple-iterator data)})))

(defn tuple->simple-iterator [graph tuple]
  (->simple-iterator (get-from-index graph tuple)))

(defn- first-key [index]
  (let [val (first index)]
    (cond-> val
      (vector? val) first)))

(defn- seek-key [index k]
  (let [key-fn (if (vector? (first index)) first identity)]
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

;; TODO integrate first-key/key-fn into iterators, maybe remove levels

(defrecord LeapIteratorCore [index stack depth max-depth]
  LeapIterator
  (key [this] (first-key index))

  (next [this]
    (if-not (at-end? this)
      (with-meta (->LeapIteratorCore (subvec index 1) stack depth max-depth) (meta this))
      this))

  (seek [this k]
    (when (seq index)
      (with-meta (->LeapIteratorCore (seek-key index k) stack depth max-depth)
        (meta this))))

  (at-end? [this] (empty? index))

  LeapLevels
  (open [this]
    (assert (< (inc depth) max-depth))
    (with-meta (->LeapIteratorCore (-> index first second vec) (conj stack index) (inc depth) max-depth)
      (meta this)))

  (up [this]
    (if (= depth 0)
      ((-> this meta :original-itr))
      (with-meta (->LeapIteratorCore (peek stack) (pop-empty stack) (dec depth) max-depth)
        (meta this))))

  (level [this] depth)

  (depth [this] max-depth))

(defmethod print-method LeapIteratorCore [_g ^java.io.Writer w]
  (.write w "#LeapIteratorCore{}"))

(defn ->leap-iterator-core [index max-depth]
  (let [itr-core (->LeapIteratorCore (vec index) [] 0 max-depth)]
    (with-meta itr-core {:original-itr #(->leap-iterator-core index max-depth)})))

(defrecord LeapIteratorAVL [index stack depth max-depth]
  LeapIterator
  (key [this] (first-key index))

  (next [this]
    (with-meta (->LeapIteratorAVL (clojure.core/next index) stack depth max-depth)
      (meta this)))

  (seek [this k]
    (when (seq index)
      (with-meta (->LeapIteratorAVL (avl/seek index k) stack depth max-depth)
        (meta this))))

  (at-end? [this] (empty? index))

  LeapLevels
  (open [this]
    (assert (< (inc depth) max-depth))
    (with-meta (->LeapIteratorAVL (-> index first second seq) (conj stack index) (inc depth) max-depth)
      (meta this)))

  (up [this]
    (if (= depth 0)
      ((-> this meta :original-itr))
      (with-meta (->LeapIteratorAVL (peek stack) (pop-empty stack) (dec depth) max-depth)
        (meta this))))

  (level [this] depth)

  (depth [this] max-depth))

(defmethod print-method LeapIteratorAVL [_g ^java.io.Writer w]
  (.write w "#LeapIteratorAvl{}"))

(defn- avl-index? [index]
  (or (nil? index)
      (instance? clojure.data.avl.AVLMap index)
      (instance? clojure.data.avl.AVLSet index)))

(defn ->leap-iterator-avl [index max-depth]
  {:pre [(avl-index? index)]}
  (let [avl-itr (->LeapIteratorAVL (seq index) [] 0 max-depth)]
    (with-meta avl-itr {:original-itr #(->leap-iterator-avl index max-depth)})))

(defrecord LeapIteratorTonsky [index stack depth max-depth]
  LeapIterator
  (key [this] (first-key index))

  (next [this]
    (with-meta (->LeapIteratorTonsky (clojure.core/next index) stack depth max-depth)
      (meta this)))

  (seek [this k]
    (when (seq index)
      (with-meta (->LeapIteratorTonsky (tonsky-map/seek index k) stack depth max-depth)
        (meta this))))

  (at-end? [this] (empty? index))

  LeapLevels
  (open [this]
    (assert (< (inc depth) max-depth))
    (with-meta (->LeapIteratorTonsky (-> index first second seq) (conj stack index) (inc depth) max-depth)
      (meta this)))

  (up [this]
    (if (= depth 0)
      ((-> this meta :original-itr))
      (with-meta (->LeapIteratorTonsky (peek stack) (pop-empty stack) (dec depth) max-depth)
        (meta this))))

  (level [this] depth)

  (depth [this] max-depth))

(defmethod print-method LeapIteratorTonsky [_g ^java.io.Writer w]
  (.write w "#LeapIteratorTonsky{}"))

(defn- tonsky-index? [index]
  (or (nil? index)
      (instance? hooray.util.persistent_map.PersistentSortedMap index)
      (instance? me.tonsky.persistent_sorted_set.PersistentSortedSet index)))

(comment
  (tonsky-index? 'foo)
  )

(defn ->leap-iterator-tonsky [index max-depth]
  {:pre [(tonsky-index? index)]}
  (let [tonsky-itr (->LeapIteratorTonsky (seq index) [] 0 max-depth)]
    (with-meta tonsky-itr {:original-itr #(->leap-iterator-tonsky index max-depth)})))

(def ^:private iterator-types #{:simple :core :avl :tonsky})

#_(s/fdef get-iterator*
    :args (s/cat :graph any? :tuple ::tuple :type iterator-types))
;; (require '[clojure.spec.test.alpha :as st])
;; (st/unstrument '(get-iterator*))

(defn- unconform-tuple [{:keys [triple triple-order]}]
  {:triple (map #(let [[type v] (get triple %)]
                   (if (= type :logic-var) v (hash v)))
                triple-order)
   :triple-order triple-order})

(comment
  (unconform-tuple '{:triple
                     {:e [:logic-var ?t],
                      :a [:literal :track/name],
                      :v [:literal "For Those About To Rock (We Salute You)"]},
                     :triple-order [:a :v :e]}))

(defn get-iterator* [graph {:keys [triple _triple-order] :as tuple} type]
  #_{:pre [(s/assert ::tuple tuple) (iterator-types type)]}
  (let [{:keys [triple] :as tuple} (cond-> tuple
                                     (map? triple) unconform-tuple)
        nb-vars (count (filter util/variable? triple))]
    (case type
      :simple (->simple-iterator (get-from-index graph tuple))
      :core (->leap-iterator-core (get-index graph tuple) nb-vars)
      :avl (->leap-iterator-avl (get-index graph tuple) nb-vars)
      :tonsky (->leap-iterator-tonsky (get-index graph tuple) nb-vars)
      (throw (ex-info "todo" {})))))

(defn set-iterator-level [itr l]
  {:pre [(and (<= 0 l) (< l (depth itr)))]}
  (cond (< l (level itr)) (set-iterator-level (open itr) l)
        (> l (level itr)) (set-iterator-level (up itr) l)
        :else itr))

(defn reset-iterator [itr]
  (set-iterator-level itr 0))
