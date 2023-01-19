(ns hooray.query
  (:require [hooray.algo.generic-join :as gj]
            [hooray.algo.hash-join :as hj]
            [hooray.algo.leapfrog :as lf]
            [hooray.db :as db]
            [hooray.db.memory]
            [hooray.db.memory.graph]
            [hooray.db.memory.graph-index]
            [hooray.graph]
            [hooray.query.spec :as hooray-spec]
            [hooray.util :as util])
  (:import (hooray.db.memory.graph MemoryGraph)
           (hooray.db.memory.graph_index MemoryGraphIndexed)
           (hooray.db.persistent.graph PersistentGraph)))

(defn- ->return-maps [{:keys [keys syms strs]}]
  (let [ks (or (some->> keys (mapv keyword))
               (some->> syms (mapv symbol))
               (some->> strs (mapv str)))]
    (fn [row]
      (zipmap ks row))))

(comment
  (def conformed-q (hooray-spec/conform-query '{:find [?name]
                                                :where
                                                [[?t :track/name "For Those About To Rock (We Salute You)"]
                                                 [?t :track/album ?album]
                                                 [?album :album/artist ?artist]
                                                 [?artist :artist/name ]
                                                 [_ :foo/bar]]
                                                :limit 12})))

(defn order-by-bindings [v var->bindings]
  (sort-by var->bindings v))

(defn compile-find [{:keys [query var->bindings] :as _compiled_q}]
  (let [find (:find query)]
    (if (seq find)
      (fn find-projection [row]
        (mapv (fn [v]
                (if (util/variable? v)
                  (nth row (get var->bindings v))
                  v))
              find))
      (throw (ex-info "`find` can't be empty!" {:query query})))))

(defn var-join-order [{:keys [where]} _db]
  (->> (mapcat identity where)
       (filter util/variable?)
       distinct
       vec))

(defn query-plan [q db]
  (let [var-join-order (var-join-order q db)]
    {:var-join-order var-join-order
     :var->bindings (zipmap var-join-order (range))}))

(defn- replace-wildcards [{:keys [where] :as q}]
  (->> (mapv #(mapv (fn [v] (if (util/wildcard? v) (symbol (str "?" (gensym))) v)) %) where)
       (assoc q :where )))

(defn unique-variable-db? [db]
  (instance? MemoryGraphIndexed (db/graph db)))

(defn compile-query [q db]
  (let [q (replace-wildcards q)
        q-plan (query-plan q db)
        conformed-q  (hooray-spec/conform-query q {:unique? (unique-variable-db? db)})]
    (-> q-plan
        (assoc :query q
               :conformed-query conformed-q))))

;; TODO move this down to the actual namespaces
(defn join-dispatch-fn [_compiled-q db] (type (db/graph db)))
(defmulti join join-dispatch-fn)

(defmethod join :default [_compiled-q db]
  (throw (ex-info "Graph type not known!!!" {:graph-type (type (db/graph db))})))

(defmethod join MemoryGraph [compiled-q db]
  (hj/join compiled-q db))

(defmethod join PersistentGraph [compiled-q {:keys [opts] :as db}]
  (let [algo (-> opts :uri-map :algo)]
    (case algo
      (nil :hash) (hj/join compiled-q db)
      :leapfrog (lf/join compiled-q db)
      :generic (gj/join compiled-q db)
      (throw (ex-info "No such algorithm known!" {:algo algo})))))

(defmethod join MemoryGraphIndexed [compiled-q {:keys [opts] :as db}]
  (let [algo (-> opts :uri-map :algo)]
    (case algo
      (nil :leapfrog) (lf/join compiled-q db)
      :generic (gj/join compiled-q db)
      (throw (ex-info "No such algorithm known!" {:algo algo})))))

(defn query [q db]
  (let [compiled-q (compile-query q db)
        find-fn (compile-find compiled-q)
        return-maps? (seq (select-keys q [:keys :syms :strs]))]
    (cond->> (join compiled-q db)
      true (map find-fn)
      return-maps? (map (->return-maps q)))))

(comment
  (require '[clojure.edn :as edn])

  (do
    (def conn(db/connect "hooray:mem://data"))
    (def conn-core (db/connect "hooray:mem:core//data"))
    (def conn-avl (db/connect "hooray:mem:avl//data"))
    (def data (edn/read-string (slurp "resources/transactions.edn")))
    (db/transact conn data)
    (db/transact conn-core data)
    (db/transact conn-avl data)

    (defn db [] (db/db conn))
    (defn db-core [] (db/db conn-core))
    (defn db-avl [] (db/db conn-avl)))

  (def q '{:find [?name ?album]
           :where [[?t :track/name "For Those About To Rock (We Salute You)" ]
                   [?t :track/album ?album]
                   [?album :album/artist ?artist]
                   [?artist :artist/name ?name]]
           :keys [name album]})

  (query q (db))
  (query q (db-core))
  (query q (db-avl)))
