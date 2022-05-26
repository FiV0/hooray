(ns hooray.db.memory.graph
  (:require [hooray.util :as util]
            [hooray.datom :as datom]))

(defrecord MemoryGraph [eav ave vea])

(defn memory-graph []
  (->MemoryGraph {} {} {}))

(def ^:private index-types [:eav :ave :vea])

(defn datom-reorder-fn [index-type]
  (case index-type
    :eav identity
    :ave (fn [[e a v]] [a v e])
    :vea (fn [[e a v]] [v e a])
    (throw (ex-info "No such index!" {}))))

(defn index-triple-insert [index [v1 v2 v3]]
  (update-in index [v1 v2] (fnil conj #{}) v3))

(defn index-triples-insert [index triples]
  (reduce index-triple-insert index triples))

(defn insert-datoms [graph datoms]
  (let [triples (map #(datom/as-vec %) datoms)]
    (reduce (fn [graph index-type]
              (update graph index-type index-triples-insert
                      (map (datom-reorder-fn index-type) triples)))
            graph index-types)))


(comment
  (def datoms (->> (range 100)
                   (partition 5)
                   (map #(apply datom/->Datom %))
                   (take 3)))

  (insert-datoms (memory-graph) datoms))
