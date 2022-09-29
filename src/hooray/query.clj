(ns hooray.query
  (:require [clojure.core.rrb-vector :as fv]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.spec.alpha :as s]
            [hooray.db :as db]
            [hooray.graph :as graph]
            [hooray.util :as util]
            [hooray.query.spec :as hooray-spec]
            [hooray.algo.binary-join :as bj]))

(defn same-triples? [pattern1 pattern2]
  (->> (map #(or (and (util/variable? %1) (util/variable? %2))
                 (= %1 %2))
            pattern1 pattern2)
       (every? true?)))

(defn pattern->indexed-map [pattern]
  (into {} (map-indexed #(vector %2 %1) pattern)))

(defn resolve-rows [db where-clauses]
  (if (satisfies? db/GraphDatabase db)
    (let [graph (db/graph db)]
      (map (fn [where-clause]
             {:pattern where-clause
              :rows (graph/resolve-triple graph where-clause)})
           where-clauses))
    (throw (ex-info "todo" {}))))

;; todo use transducer
(defn row-filter [[e a v :as where-clause] rows]
  (cond->> rows
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
  (row-filter '[_ :likes ?e] data))

(defn- shrink-data-row [keep-pattern row]
  (->> (map vector keep-pattern row)
       (reduce (fn [row [keep value]] (if keep (conj row value) row)) [])))

(defn remove-constants [{:keys [pattern rows]}]
  (let [keep-pattern (map util/variable? pattern)
        new-pattern (->> (map vector keep-pattern pattern)
                         (reduce (fn [p [keep v]] (if keep (conj p v) p)) []))
        new-rows (map (partial shrink-data-row keep-pattern) rows)]
    {:pattern new-pattern
     :rows new-rows}))

(comment
  (def the-filter '[_ :likes ?e])
  (remove-constants {:pattern the-filter :rows (row-filter the-filter data)}))

(defn input->rows [input where-clauses]
  (let [res (cond
              (satisfies? db/Database input)
              (resolve-rows input where-clauses)

              (or (list? input) (vector? input))
              (map (fn [where-clause]
                     {:pattern where-clause
                      :rows (row-filter where-clause input)})
                   where-clauses)

              :else (throw (ex-info "Not supported input:" {:input input})))]
    (->> res
         (map remove-constants)
         (map #(update % :pattern pattern->indexed-map)))))

;; what follows needs to be cleaned up and rewritten
;; it currently makes some assumptions about the structure
;; of the patterns (for examples duplicates in the same clause won't work)

;; TODO look at proper datalog resolving

(defn- join-rows? [data-row1 pattern-index1 data-row2 pattern-index2]
  {:pre [(= (set (keys pattern-index1)) (set (keys pattern-index2)))]}
  (loop [vars (keys pattern-index2)]
    (if-let [var (first vars)]
      (if (= (nth data-row1 (get pattern-index1 var))
             (nth data-row2 (get pattern-index2 var)))
        (recur (rest vars))
        false)
      true)))

(comment
  (join-rows? [3 4 5] {'?e 0} [1 2 3] {'?e 2})
  (join-rows? [3 4 5] {'?e 0 '?f 1} [1 2 3] {'?e 2 '?f 1})
  (join-rows? [3 4 5] {'?e 0} [1 2 5] {'?e 2})
  (join-rows? [3 4 5] {'?e 0} [1 2 5] {'?f 2 '?e 1})
  (join-rows? [3 4 5] {} [1 2 5] {})

  )

(defn- join-row* [data-row1 data-row2 keep-pattern]
  #_(fv/catvec data-row1 (shrink-data-row keep-pattern data-row2))
  (concat data-row1 (shrink-data-row keep-pattern data-row2)))

;; TODO the key intersection can probably be done more efficiently
;; as we can assume that the second pattern is small
(defn- join-row [row1 pattern-index1 row2 pattern-index2 keep-pattern]
  (when (join-rows? row1 pattern-index1 row2 pattern-index2)
    (join-row* row1 row2 keep-pattern)))

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
  (join-row [3 4 5] {'e 0} [1 2 3] {'e 2} [true true false])
  (join-row [3 4 5] {'e 0} [1 2 5] {'e 2} [true true false])

  (cart-f [[3 4 5]
           [5 6 8]]
          [[1 2 3]
           [6 7 3]
           [8 9 5]]
          #(join-row %1 {'e 0} %2 {'e 2} [true true false])))

(defn join [{rows1 :rows pattern-index1 :pattern} {rows2 :rows pattern-index2 :pattern}]
  (let [p1-keys (keys pattern-index1)
        p2-keys (keys pattern-index2)
        inter (set/intersection (set p1-keys) (set p2-keys))
        subset-p1 (select-keys pattern-index1 inter)
        subset-p2 (select-keys pattern-index2 inter)
        row2-keep-pattern (map (fn [[k _v]] ((complement inter) k)) pattern-index2)
        new-pattern-index (->> (remove (fn [[k _]] (inter k)) pattern-index2)
                               (sort #(compare (second %1) (second %2)))
                               (reduce (fn [pattern-index [k _]] (assoc pattern-index k (count pattern-index))) pattern-index1))]
    {:pattern new-pattern-index
     :rows (cart-f rows1 rows2 #(join-row %1 subset-p1 %2 subset-p2 row2-keep-pattern))}))

(comment
  (def data1 '[[sally :age 21]
               [fred :age 42]
               [ethel :age 42]])
  (def data2 '[[fred :likes pizza]
               [sally :likes opera]
               [ethel :likes sushi]])

  (join {:pattern (pattern->indexed-map '[?e ?a1 ?age])
         :rows data1}
        {:pattern (pattern->indexed-map '[?e ?a2 ?food])
         :rows  data2})
  (join {:pattern (pattern->indexed-map '[?e ?a1 ?age])
         :rows data1}
        {:pattern (pattern->indexed-map '[?e1 ?a2 ?food])
         :rows data2}))


(defn compute-find [find-clause {rows :rows pattern-index :pattern}]
  {:pre [(set/subset? (set find-clause) (set (keys pattern-index)))]}
  (let [select (fn [row] (reduce (fn [result-row var]
                                   (conj result-row (nth row (get pattern-index var))))
                                 []
                                 find-clause)) ]
    (map select rows)))

(defn- unique-variables? [clause]
  (let [variables (filter util/variable? clause)]
    (= variables (distinct variables))))


(defn make-wildcards-unique [clause]
  (mapv #(if (util/wildcard? %) (gensym "wildcard_") %) clause))

(defn cleanup-where [clauses]
  (let [res (mapv make-wildcards-unique clauses)]
    (loop [clauses res]
      (if-let [clause (first clauses)]
        (if (unique-variables? clause)
          (recur (rest clauses))
          (throw (ex-info "Where-clause needs distinct variables!" {:clause clause})))
        res))))

(defn- ->return-maps [{:keys [keys syms strs]}]
  (let [ks (or (some->> keys (mapv keyword))
               (some->> syms (mapv symbol))
               (some->> strs (mapv str)))]
    (fn [row]
      (zipmap ks row))))

(comment
  (cleanup-where '[[_ :foo ?e]
                   [?e :bar 1]]))

;; TODO add spec for inputs
;; TODO think about how multiple inputs are handled
;; TODO replace wildcards with unique symbols
;; TODO assert unique symbols per where-clause
;; check with datomic
(defn query2 [query input]
  (let [{:keys [find where]} query
        where (cleanup-where where)
        input-data (input->rows input where)]
    (->> (reduce join input-data)
         (compute-find find))))
(comment
  (query2 '{:find [?person ?age]
            :where [[?person :age ?age]
                    [?person :likes pizza]]}
          data))



(defn logic-var? [v]
  (and (simple-symbol? v)
       (comp #(str/starts-with? % "?") name)))

(defn wildcard? [v] (= v '_))

(defn literal? [v]
  (not (or (wildcard? v) (logic-var? v)))
  #_(complement logic-var?))

(defn- vars-from-triple [{:keys [e a v]}]
  (->> [e a v]
       (filter (comp #{:logic-var} first))
       (map second)))

(defn var-join-order [{:keys [where] :as q} _db]
  (->> where
       (filter (comp #{:triple} first))
       (mapcat (comp vars-from-triple second))
       dedupe))

(defn query-plan [q db]
  (let [var-join-order (var-join-order q db)]
    {:var-join-order var-join-order
     :var->bindings (zipmap var-join-order (range))}))

(comment
  (def conformed-q (hooray-spec/conform-query '{:find [?name]
                                                :where
                                                [[?t :track/name "For Those About To Rock (We Salute You)"]
                                                 [?t :track/album ?album]
                                                 [?album :album/artist ?artist]
                                                 [?artist :artist/name ]
                                                 [_ :foo/bar]]
                                                :limit 12}))
  (query-plan conformed-q nil))

;; index-fn should follow the convention of [opts, next var bound ....]

(defn ->unary-index-fn [{:keys [e a v]} db]
  (fn [_]
    (graph/resolve-triple (db/graph db) [e a v])))

(defn ->binary-index-fn [{:keys [e a v]} db]
  )

(defn ->ternary-index-fn [{:keys [e a v]} db]
  {:pre [(assert (every? logic-var? [e a v]))]}



  )

(defn order-by-bindings [v var->bindings]
  (sort-by var->bindings v))

(defn ->index-fn [{:keys [e a v]} var->joins db]
  (let [vars (-> (filter logic-var? [e a v])
                 (sort-by ))])
  )

(defn var->joins [{:keys [where] :as q} db {:keys [var->bindings] :as _q-plan}]
  (let [triples (->> (filter (comp #{:triple} first) where)
                     (map second))]))

(defn compile-query [q db]
  (let [q-plan (query-plan q db)]
    (->
     q-plan
     #_(assoc :var->joins (var->joins q db q-plan))
     (assoc :query q))))


(defn compile-find [{:keys [query var->bindings] :as _compiled_q}]
  (let [find (:find query)]
    (if (seq find)
      (fn find-projection [row]
        (mapv (fn [v]
                (if (logic-var? v)
                  (nth row (get var->bindings v))
                  v))
              find))
      (throw (ex-info "`find` can't be empty!" {:query query})))))

(defn var-join-order2 [{:keys [where]} db]
  (->> (mapcat identity where)
       (filter logic-var?)
       dedupe
       vec))

(defn query-plan2 [q db]
  (let [var-join-order (var-join-order2 q db)]
    {:var-join-order var-join-order
     :var->bindings (zipmap var-join-order (range))}))

(defn compile-query2 [q db]
  (let [q-plan (query-plan2 q db)]
    (-> q-plan
        (assoc :query q))))

(defn- ->return-maps [{:keys [keys syms strs]}]
  (let [ks (or (some->> keys (mapv keyword))
               (some->> syms (mapv symbol))
               (some->> strs (mapv str)))]
    (fn [row]
      (zipmap ks row))))

(defn query [q db]
  (let [#_#_conformed-q (hooray-spec/conform-query q)
        compiled-q (compile-query2 q db)
        find-fn (compile-find compiled-q)
        return-maps? (seq (select-keys q [:keys :syms :strs]))]
    (cond->> (bj/join compiled-q db)
      true (map find-fn)
      return-maps? (map (->return-maps q)))))

(comment
  (require '[clojure.edn :as edn]
           '[hooray.db :as db]
           '[hooray.graph :as g])

  (defn wrap-in-adds [tx-data]
    (map #(vector :db/add %) tx-data))

  (def conn (db/connect "hooray:mem://data"))
  (def data (edn/read-string (slurp "resources/transactions.edn")))
  (db/transact conn data)

  (defn db [] (db/db conn))

  (query '{:find [?name ?album]
           :where [[?t :track/name "For Those About To Rock (We Salute You)" ]
                   [?t :track/album ?album]
                   [?album :album/artist ?artist]
                   [?artist :artist/name ?name]]
           :keys [name album]}
         (db))

  (g/resolve-triple  (:graph (db)) '[?t :track/name "For Those About To Rock (We Salute You)" ])


  )
