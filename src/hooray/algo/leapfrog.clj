(ns hooray.algo.leapfrog
  (:require [hooray.util :as util]
            [hooray.db :as db]
            [hooray.graph :as graph]
            [hooray.db.memory.graph-index :as g-index]
            [medley.core :refer [map-kv]]))

;; leapfrog triejoin

;; what do I need ?
;; var -> indices of clause
;; clause + var -> level
;; construction of triples for iterators

;; TODO
;; Multiple variable appearance in triples

(comment
  (require '[clojure.spec.alpha :as s]
           '[hooray.query.spec :as h-spec])
  (def q '{:find [?name ?album]
           :where [[?t :track/name "For Those About To Rock (We Salute You)" ]
                   [?t :track/album ?album]
                   [?album :album/artist ?artist]
                   [?artist :artist/name ?name]]
           ;; :keys [name album]
           })

  (def q-conformed (s/conform ::h-spec/query q)))

(defn- filter-triples [where]
  (filter (comp #{:triple} first) where))

(defn- logic-var? [[type _value]]
  (= :logic-var type))

(defn- ->value [[_ value]] value)

(defn- var->indices-map [where]
  (->> (map-indexed #(vector %1 (second %2)) where)
       (reduce (fn [res [i {:keys [e a v]}]]
                 (cond-> res
                   (logic-var? e) (update (->value e) (fnil conj []) i)
                   (logic-var? a) (update (->value a) (fnil conj []) i)
                   (logic-var? v) (update (->value v) (fnil conj []) i))) {})))

(comment
  (var->indices-map (:where q-conformed)))

(defn- triple->vars [{:keys [e a v]}]
  (reduce #(if (logic-var? %2) (conj %1 (second %2)) %1) [] [e a v]))

(defn triple->sorted-vars [triple var->join-index]
  (->> (triple->vars triple)
       (sort-by var->join-index)))

(defn- triple+var->level-map [where var->join-index]
  (->> (map-indexed #(vector %1 (second %2)) where)
       (reduce (fn [res [i triple]]
                 (->> (map-indexed vector (triple->sorted-vars triple var->join-index))
                      (reduce (fn [res [level var]] (assoc res [i var] level)) res))) {})))

(defn- var->join-index [var-join-order]
  (zipmap var-join-order (range)))

(comment
  (def vars (keys (var->indices-map (:where q-conformed))))
  (def var->j-index (var->join-index vars))
  (triple+var->level-map (:where q-conformed) var->j-index))

(defn triple->tuple
  ([triple] {:triple triple :triple-order [:e :a :v]})
  ([{:keys [e a v] :as _triple} var->join-index]
   (let [[literals vars] (->> (map vector [e a v] [:e :a :v])
                              ((juxt remove filter) (comp logic-var? first)))
         vars (sort-by #(-> %1 first second var->join-index) vars)]
     {:triple (into (vec (map first literals)) (map first vars))
      :triple-order (into (vec (map second literals)) (map second vars))})))

(comment
  (triple->tuple (-> q-conformed :where first second) var->j-index)
  (triple->tuple (-> q-conformed :where second second) var->j-index)
  (triple->tuple (-> q-conformed :where (nth 2) second) var->j-index))

(defn- sort-indices [indices iterators]
  (let [keyfn (zipmap indices (map g-index/key iterators))]
    (sort-by keyfn indices)))

(defn- var-indices-sorting [var indices iterators triple-idx+var->level]
  (let [iterators (map #(g-index/set-iterator-level %2 (triple-idx+var->level [var %1])) indices iterators)]
    (sort-indices indices iterators)))

(defn- initial-indices-sorting [var->indices triple-idx+var->level idx->iterators]
  (map-kv (fn [var indices]
            [var (var-indices-sorting var indices (map idx->iterators indices) triple-idx+var->level)])
          var->indices))

(defn join [{:keys [query var-join-order var->bindings] :as _compiled-q} db]
  (if-let [where (:where query)]
    (let [graph (db/graph db)
          triple-idx+var->level (triple+var->level-map where var->bindings)
          tuples (->> (filter (comp #{:triple} first) where)
                      (mapv (comp triple->tuple second)))
          idx->trie-iterators (->> (map #(graph/get-iterator graph %) tuples)
                                   (zipmap (range)))
          var->indices (-> (var->indices-map where)
                           (initial-indices-sorting triple-idx+var->level idx->trie-iterators))]
      (loop []
        ;;TODO
        )
      )
    (throw (ex-info "Query must contain where clause!" {:query query}))))
