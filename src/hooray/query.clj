(ns hooray.query
  (:require [clojure.string :as str]
            [hooray.algo.binary-join :as bj]
            [hooray.algo.leapfrog :as lf]
            [hooray.db :as db]
            [hooray.db.memory.graph]
            [hooray.db.memory.graph-index]
            [hooray.graph :as graph]
            [hooray.query.spec :as hooray-spec]
            [hooray.util :as util])
  (:import (hooray.db.memory.graph MemoryGraph)
           (hooray.db.memory.graph_index MemoryGraphIndexed)))

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

(defn var-join-order [{:keys [where] :as _q} _db]
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
  ;; FIXME
  )

(defn ->ternary-index-fn [{:keys [e a v]} db]
  {:pre [(assert (every? logic-var? [e a v]))]}
  ;; FIXME
  )

(defn order-by-bindings [v var->bindings]
  (sort-by var->bindings v))

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

(defn var-join-order2 [{:keys [where]} _db]
  (->> (mapcat identity where)
       (filter logic-var?)
       distinct
       vec))

(defn query-plan2 [q db]
  (let [var-join-order (var-join-order2 q db)]
    {:var-join-order var-join-order
     :var->bindings (zipmap var-join-order (range))}))

(defn compile-query2 [q db]
  (let [q-plan (query-plan2 q db)
        conformed-q (hooray-spec/conform-query q)]
    (-> q-plan
        (assoc :query q
               :conformed-query conformed-q))))

;; TODO move this down to the actual namespaces
(defmulti join (fn [_compiled-q db] (type (db/graph db))))

(defmethod join MemoryGraph [compiled-q db]
  (bj/join compiled-q db))

(defmethod join MemoryGraphIndexed [compiled-q db]
  (lf/join compiled-q db))

(defn query [q db]
  (let [compiled-q (compile-query2 q db)
        find-fn (compile-find compiled-q)
        return-maps? (seq (select-keys q [:keys :syms :strs]))]
    (cond->> (join compiled-q db)
      true (map find-fn)
      return-maps? (map (->return-maps q)))))

(comment
  (require '[clojure.edn :as edn]
           '[hooray.db :as db]
           '[hooray.graph :as g])

  (defn wrap-in-adds [tx-data]
    (map #(vector :db/add %) tx-data))

  (do
    (def conn (db/connect "hooray:mem://data"))
    (def conn-core (db/connect "hooray:mem:core//data"))
    (def conn-avl (db/connect "hooray:mem:avl//data"))
    (def data (edn/read-string (slurp "resources/transactions.edn")))
    (db/transact conn data)
    (db/transact conn-core data)
    (db/transact conn-avl data)

    (defn db [] (db/db conn))
    (defn db-core [] (db/db conn-core))
    (defn db-avl [] (db/db conn-avl))
    )

  (def q '{:find [?name ?album]
           :where [[?t :track/name "For Those About To Rock (We Salute You)" ]
                   [?t :track/album ?album]
                   [?album :album/artist ?artist]
                   [?artist :artist/name ?name]]
           :keys [name album]})

  (query q (db))
  (query q (db-core))
  (query q (db-avl))

  (g/resolve-triple  (:graph (db)) '[?t :track/name "For Those About To Rock (We Salute You)" ])

  )
