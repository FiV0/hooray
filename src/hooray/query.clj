(ns hooray.query
  (:require [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [clojure.core.rrb-vector :as fv]
            [hooray.db :as db]
            [hooray.util :as util]))

(defn resolve-data [db where-clauses]
  (throw (ex-info "todo" {})))

(declare data-filter)

(defn input->data [input where-clauses]
  (cond
    (satisfies? db/Database input)
    (resolve-data input where-clauses)

    (or (list? input) (vector? input))
    (map (fn [where-clause]
           {:pattern where-clause
            :data (data-filter where-clause input)})
         where-clauses)

    :else (throw (ex-info "Not supported input:" {:input input}))))



;; what follows needs to be cleaned up and rewritten
;; it currently makes some assumptions about the structure
;; of the patterns (for examples duplicates in the same clause won't work)

;; todo use transducer
(defn data-filter [[e a v :as where-clause] data]
  (cond->> data
    (util/constant? e) (filter (fn [[e1 _ _]] (= e e1)))
    (util/constant? a) (filter (fn [[_ a1 _]] (= a a1)))
    (util/constant? v) (filter (fn [[_ _ v1]] (= v v1)))))

(comment
  (def data '[[sally :age 21]
              [fred :age 42]
              [ethel :age 42]
              [fred :likes pizza]
              [sally :likes opera]
              [ethel :likes sushi]])
  (data-filter '[_ :likes ?e] data))

(defn- shrink-data-row [keep-pattern data-row]
  (->> (map vector keep-pattern data-row)
       (reduce (fn [row [keep value]] (if keep (conj row value) row)) [])))

(defn remove-constants [{:keys [pattern data]}]
  (let [keep-pattern (map util/variable? pattern)
        new-pattern (->> (map vector keep-pattern pattern)
                         (reduce (fn [p [keep v]] (if keep (conj p v) p)) []))
        new-data (map (partial shrink-data-row keep-pattern) data)]
    {:pattern new-pattern
     :data new-data}))

(comment
  (def the-filter '[_ :likes ?e])
  (remove-constants {:pattern the-filter :data (data-filter the-filter data)}))

(defn- join-rows? [data-row1 variable-index-map1 data-row2 variable-index-map2]
  (loop [vars (keys variable-index-map2)]
    (if-let [var (first vars)]
      (if (= (nth data-row1 (get variable-index-map1 var))
             (nth data-row2 (get variable-index-map2 var)))
        (recur (rest vars))
        false)
      true)))

(comment
  (join-rows? [3 4 5] {'?e 0} [1 2 3] {'?e 2})
  (join-rows? [3 4 5] {'?e 0 '?f 1} [1 2 3] {'?e 2 '?f 1})
  (join-rows? [3 4 5] {'?e 0} [1 2 5] {'?e 2})
  (join-rows? [3 4 5] {'?e 0} [1 2 5] {'?f 2 '?e 1}))

(defn- join-row* [data-row1 data-row2 keep-pattern]
  #_(fv/catvec data-row1 (shrink-data-row keep-pattern data-row2))
  (concat data-row1 (shrink-data-row keep-pattern data-row2)))

(defn- join-row [data-row1 data-row2 variable-index-map1 variable-index-map2 keep-pattern]
  (when (join-rows? data-row1 variable-index-map1 data-row2 variable-index-map2)
    (join-row* data-row1 data-row2 keep-pattern)))

(defn cart-f [coll1 coll2 f]
  (if-let [v1 (first coll1)]
    (let [res (cart-f (rest coll1) coll2 f)]
      (loop [c coll2 r res]
        (if-let [v2 (first c)]
          (if-let [new-v (f v1 v2)]
            (recur (rest c) (conj r new-v))
            (recur (rest c) r))
          r)))
    []))

(comment
  (join-row [3 4 5] [1 2 3] {'e 0} {'e 2} [true true false])
  (join-row [3 4 5] [1 2 5] {'e 0} {'e 2} [true true false])

  (cart-f [[3 4 5]
           [5 6 8]]
          [[1 2 3]
           [6 7 3]
           [8 9 5]]
          #(join-row %1 %2 {'e 0}
                     {'e 2}
                     [true true false])))

;; pattern2 should only contain variables
(defn join [pattern1 data1 pattern2 data2]
  (let [pattern2-set (set pattern2)
        inter (set/intersection (set pattern1) pattern2-set)
        variable-index-map2 (into {} (map-indexed #(vector %2 %1) pattern2))
        variable-index-map1 (->> (map-indexed #(vector %2 %1) pattern1)
                                 (filter (fn [[var _]] (contains? variable-index-map2 var)))
                                 (into {}))
        variable-index-map2 (select-keys variable-index-map2 (keys variable-index-map1))
        new-pattern (vec (concat pattern1 (remove inter pattern2)))
        data-row2-keep-pattern (map (complement inter) pattern2)]
    {:pattern new-pattern
     :data (cart-f data1 data2 #(join-row %1 %2 variable-index-map1 variable-index-map2 data-row2-keep-pattern))}))

(comment
  (def data1 '[[sally :age 21]
               [fred :age 42]
               [ethel :age 42]])
  (def data2 '[[fred :likes pizza]
               [sally :likes opera]
               [ethel :likes sushi]])

  (join '[?e ?a1 ?age] data1 '[?e ?a2 ?food] data2))


(defn compute-find [find-clause {:keys [pattern data]}]
  {:pre [(set/subset? (set find-clause) (set pattern))]}
  (let [variable-index-map (into {} (map-indexed #(vector %2 %1) pattern))]
    (map (fn [row]
           (reduce (fn [result-row var] (conj result-row (nth row (get variable-index-map var)))) [] find-clause))
         data)))

;; end of part to cleanup

;; TODO add spec for inputs
(defn query [query input]
  (let [{:keys [find where]} query
        input-data (input->data input where)]
    (->> (reduce (fn [{pattern1 :pattern data1 :data}
                      {pattern2 :pattern data2 :data}]
                   (-> (join pattern1 data1 pattern2 data2)
                       remove-constants))
                 input-data)
         (compute-find find))))


(comment
  (query '{:find [?person ?age]
           :where [[?person :age ?age]
                   [?person :likes pizza]]}
         data))
