(ns hooray.db.persistent.transact
  (:require [clojure.spec.alpha :as s]
            [hooray.util :as util]
            [hooray.db.persistent.protocols :as proto]
            [hooray.txn]))

;; TODO unify with graph_index
(defn- map->triples [m ts]
  (let [eid (or (:db/id m) (random-uuid))]
    (->> (dissoc m :db/id)
         (map (fn [[k v]] (vector eid k v ts true))))))

(defn transaction->triples [transaction ts]
  (cond
    (map? transaction) (map->triples transaction ts)
    (= :db/add (first transaction)) [(vec (concat (rest transaction) [ts true]))]
    (= :db/retract (first transaction)) [(vec (concat (rest transaction) [ts false]))]))

;; indices needed
;; :e :a :v
;; :ea :ev :ae :av :ve :va
;; :eav :eva :aev :ave :vea :vae
;; TODO check if one can iterate cheaply over prefixes

(declare transact*)

(defn transact [conn tx-data]
  {:pre [(s/assert :hooray/tx-data tx-data)]}
  (let [ts (util/now)
        triples (mapcat #(transaction->triples % ts) tx-data)]
    (transact* conn triples)))
