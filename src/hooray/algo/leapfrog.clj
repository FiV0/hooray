(ns hooray.algo.leapfrog
  (:require [hooray.util :as util]
            [hooray.db :as db]
            [hooray.db.memory.graph-index :as g-index]))

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

(defn triple->sorted-vars [triple var->bindings]
  (->> (triple->vars triple)
       (sort (comparator #(< (var->bindings %1) (var->bindings %2))))))

(defn- triple+var->level-map [where var->bindings]
  (->> (map-indexed #(vector %1 (second %2)) where)
       (reduce (fn [res [i triple]]
                 (->> (map-indexed vector (triple->sorted-vars triple var->bindings))
                      (reduce (fn [res [level var]] (assoc res [i var] level)) res))) {})))

(comment
  (def vars (keys (var->indices-map (:where q-conformed))))
  (def var->bindings (zipmap vars (range)))
  (triple+var->level-map (:where q-conformed) var->bindings)

  )


(defn join [{:keys [query var-join-order] :as _compiled-q} db]
  (if-let [where (:where query)]
    (if (seq where)
      (let [graph (db/graph db)]
        ;;TODO
        )
      (throw (ex-info "Where can't be empty!" {:where where})))
    (throw (ex-info "Query must contain where clause!" {:query query}))))
