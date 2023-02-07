(ns hooray.tuple
  (:refer-clojure :exclude [concat])
  (:require [clojure.set :as set]
            [hooray.util :as util]))

;; what we want
;; selection : TupleBag , pred -> TupleBag
;; projection : TupleBag , names -> TupleBag
;; concat : TupleBag , TupleBag -> TupleBag
;; TupleBag , iterable , name -> TupleBag

(declare ->Tuple)
(deftype Tuple [v]
  clojure.lang.Seqable
  (seq [this] (seq v))
  clojure.lang.Indexed
  (nth [_ i] (nth v i))
  (nth [_ _i _not-found] (util/unsupported-ex))
  clojure.lang.IPersistentVector
  (assocN [_ _index _object] (util/unsupported-ex))
  (cons [_ object] (->Tuple (conj v object)))
  (length [_] (count v))
  clojure.lang.Counted
  (count [_] (count v))
  java.lang.Object
  (toString [_] (str v)))

(defmethod print-method Tuple [t ^java.io.Writer w]
  (.write w "hooray.tuple.Tuple")
  (.write w (str t)))

(defn tuple [v] (Tuple. v))

(comment
  (def t (Tuple. [1 2 3]))
  (def t2 (Tuple. [1 2 3]))
  (into t t2)
  (nth t 1)
  (count t)
  (conj t 4)
  (nth (conj t 4) 0)
  (nth (conj t 4) 3))

(defrecord TupleBag [size name->idx tuples])

(defn tuple-bag
  ([] (->TupleBag 0 {} []))
  ([names tuples]
   (assert (= (count names) (count (first tuples))))
   (->TupleBag (count tuples) (zipmap names (range)) tuples)))

(defn select [{:keys [name->idx tuples] :as _tuple-bag} pred]
  (let [res (filter pred tuples)]
    (->TupleBag (count res) name->idx (vec res))))

(defn- project-tuple [t idxs]
  (tuple (vec (map #(nth t %) idxs))))

(defn project [{:keys [name->idx tuples] :as _tuple-bag} names]
  (assert (set/subset? (set names) (set (keys name->idx))))
  (let [names->idxs (->> (select-keys name->idx names)
                         (sort-by val))
        idxs (map val names->idxs)
        new-names-idxs (->> (map-indexed (fn [i [name _]] [name i]) names->idxs)
                            (into {}))
        new-tuples (mapv #(project-tuple % idxs) tuples)]
    (->TupleBag (count new-tuples) new-names-idxs new-tuples)))

(defn concat
  ([tuple-bag] tuple-bag)
  ([{tuples1 :tuples size1 :size name->idx1 :name->idx :as _tb1}
    {tuples2 :tuples size2 :size name->idx2 :name->idx :as _tb2}]
   (assert (= size1 size2))
   (assert (empty? (set/intersection (set (keys name->idx1)) (set (keys name->idx2)))))
   (->TupleBag size1
               (into name->idx1 (update-vals name->idx2 (partial + (count name->idx1))))
               (mapv #(into %1 %2) tuples1 tuples2)))
  ([tb1 tb2 & remainder]
   (concat (concat tb1 tb2) remainder)))

(defn add [{:keys [size name->idx tuples] :as _tuple-bag} s name]
  (assert (= size (count s)))
  (->TupleBag size (assoc name->idx name (count name->idx)) (mapv #(conj %1 %2) tuples s)))

(comment
  (def tb1 (tuple-bag [:foo :bar] [(tuple [1 2]) (tuple [3 4]) (tuple [6 5])]))
  (def tb2 (tuple-bag ["foo" "bar"] [(tuple [1 2]) (tuple [3 4]) (tuple [6 5])]))
  (select tb1 #(even? (nth % 1)))
  (project tb1 [:foo])
  (concat tb1 tb1)
  (concat tb1 tb2 (tuple-bag ['foo 'bar] [(tuple [1 2]) (tuple [3 4]) (tuple [6 5])]))
  (add tb1 [1 2 3] "foo"))
