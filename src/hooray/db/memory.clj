(ns hooray.db.memory
  (:require [clojure.spec.alpha :as s]
            [hooray.datom :as datom :refer [->Datom]]
            [hooray.db :as db]
            [hooray.db.memory.bitemp-graph :as bi-graph]
            [hooray.db.memory.graph :as mem-graph]
            [hooray.db.memory.graph-index :as g-index]
            [hooray.graph :as graph]
            [hooray.txn]
            [hooray.util :as util])
  (:import (hooray.db.memory.graph MemoryGraph)
           (hooray.db.memory.graph_index MemoryGraphIndexed)
           (java.io Closeable)))

(defrecord MemoryDatabase [graph history timestamp opts]
  db/Database
  (as-of [this t] (throw (ex-info "todo" {})))
  (as-of-t [this] timestamp)
  (entity [this eid]
    (if (instance? MemoryGraph this)
      (mem-graph/entity graph eid)
      (util/unsupported-ex)))
  (get-comp [this] compare)
  (get-hash-fn [this] hash)

  db/GraphDatabase
  (graph [this] graph))

(defmethod print-method MemoryDatabase [{:keys [graph timestamp] :as db} ^java.io.Writer w]
  (.write w "#MemoryDatabase{")
  (.write w (str "graph=" (type graph) ","))
  (.write w (str "timestamp=" timestamp))
  (.write w "}"))

(declare transact*)

(defrecord MemoryConnection [name state type]
  db/Connection
  (get-name [this] name)
  (db [this] (-> this :state deref :db))
  (transact [this tx-data] (transact* this tx-data))

  Closeable
  (close [_] ;; a no-op for memory databases
    nil))

(defmethod db/connect* :mem [{:keys [name type sub-type] :as uri-map}]
  (let [opts {:uri-map uri-map}
        db (case sub-type
             (nil :graph) (->MemoryDatabase (mem-graph/memory-graph) [] (util/now) opts)
             (:core :avl :tonsky) (->MemoryDatabase (g-index/memory-graph {:type sub-type}) [] (util/now) opts)
             (throw (util/illegal-ex (str "No such db sub-type " sub-type))))]
    (->MemoryConnection name (atom {:db db}) type)))


(defmethod db/connect* :bi-mem [{:keys [name type] :as uri-map}]
  (let [opts {:uri-map uri-map}
        db (->MemoryDatabase (bi-graph/memory-bitemp-graph) [] (util/now) opts)]
    (->MemoryConnection name (atom {:db db}) type)))

(defn- map->datoms [m ts]
  (let [eid (or (:db/id m) (random-uuid))]
    (->> (dissoc m :db/id)
         (map (fn [[k v]] (->Datom eid k v ts true))))))

(defn transaction->datoms [transaction ts]
  (cond
    (map? transaction) (map->datoms transaction ts)
    (= :db/add (first transaction)) [(apply ->Datom (concat (rest transaction) [ts true]))]
    (= :db/retract (first transaction)) [(apply ->Datom (concat (rest transaction) [ts false]))]))

(defn transact* [{:keys [state type] :as _connection} tx-data]
  (case type
    :mem (s/assert :hooray/tx-data tx-data)
    :bi-mem nil
    (throw (ex-info "No such connection type!" {:type type})))
  (let [ts (util/now)
        ;; datoms (mapcat #(transaction->datoms % ts) tx-data)
        [{db-before :db} {db-after :db}]
        (swap-vals! state
                    (fn [{db-before :db}]
                      (let [{:keys [graph history opts]} db-before
                            new-graph (graph/transact graph tx-data ts)]
                        {:db (->MemoryDatabase new-graph (conj history db-before) ts opts)})))]
    {:db-before db-before
     :db-after db-after
     ;; :tx-data datoms
     }))

(comment
  (def conn (db/connect "hooray:mem://data"))
  (def conn (db/connect "hooray:mem:core//data"))
  (def conn (db/connect "hooray:mem:avl//data"))

  (def data [{:db/id "foo"
              :foo/bar "x"}
             [:db/add "foo" :is/cool true]])

  (db/transact conn data)

  (-> conn db/db (db/entity "foo"))


  )
