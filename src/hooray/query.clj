(ns hooray.query
  (:require [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [clojure.core.rrb-vector :as fv]
            [hooray.db :as db]))

(defn resolve-data [db where-clauses]
  (throw (ex-info "todo" {})))

(defn input->data [input where-clauses]
  (cond
    (satisfies? input db/Database) (resolve-data input where-clauses)
    (or (list? input) (vector? input)) input
    :else (throw (ex-info "Not supported input:" {:input input}))))


(defn variable? [v]
  (and (symbol? v) (= \? (first (name v)))))

(defn wildcard? [v]
  (= v '_))

(defn constant? [v]
  (not (or (wildcard? v) (variable? v))))

;; todo use transducer
(defn data-filter [[e a v :as where-clause] data]
  (cond->> data
    (constant? e) (filter (fn [[e1 _ _]] (= e e1)))
    (constant? a) (filter (fn [[_ a1 _]] (= a a1)))
    (constant? v) (filter (fn [[_ _ v1]] (= v v1)))))

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

(defn remove-constants [pattern data]
  (let [keep-pattern (map variable? pattern)
        new-pattern (->> (map vector keep-pattern pattern)
                         (reduce (fn [p [keep v]] (if keep (conj p v) p)) []))
        new-data (map (partial shrink-data-row keep-pattern) data)]
    {:pattern new-pattern
     :data new-data}))

(comment
  (def the-filter '[_ :likes ?e])
  (->> data
       (data-filter the-filter)
       (remove-constants the-filter)))

(defn- join-rows? [data-row1 variable-index-map1 data-row2 ])


;; pattern2 should only contain variables
(defn join [pattern1 data1 pattern2 data2]
  (let [pattern2-set (set pattern2)
        inter (set/intersection (set pattern1) pattern2-set)
        variable-index-map2 (into {} (map-indexed #(vector %2 %1) pattern2))
        variable-index-map1 (->> (map-indexed #(vector %2 %1) pattern2)
                                 (keep (fn [[var _]] (contains? variable-index-map2 var)))
                                 (into {}))
        new-pattern (fv/catvec pattern1 (filter inter pattern2))
        ]
    {:pattern new-pattern}
    ))




;; TODO add spec for inputs
(defn query [query input]
  (let [{:keys [find where]} query
        input-data (input->data input where)]

    ))
