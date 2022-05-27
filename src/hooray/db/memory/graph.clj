(ns hooray.db.memory.graph
  (:require [hooray.util :as util :refer [dissoc-in]]
            [hooray.datom :as datom]))

(defrecord MemoryGraph [eav ave vea])

(defn memory-graph []
  (->MemoryGraph {} {} {}))

(def ^:private index-types [:eav :ave :vea])

(defn datom-reorder-fn [index-type]
  (case index-type
    :eav identity
    :ave (fn [[e a v tx added]] [a v e tx added])
    :vea (fn [[e a v tx added]] [v e a tx added])
    (throw (ex-info "No such index!" {}))))

(defn index-triple-add [index [v1 v2 v3]]
  (update-in index [v1 v2] (fnil conj #{}) v3))

(defn index-triple-retract [index [v1 v2 v3]]
  (let [new-v3s (disj (get-in index [v1 v2]) v3)]
    (if (seq new-v3s)
      (assoc-in index [v1 v2] new-v3s)
      (dissoc-in index [v1 v2]))))

(defn index-triple [index [_ _ _ _ added :as triple]]
  (if added
    (index-triple-add index triple)
    (index-triple-retract index triple)))

(defn index-triples [index triples]
  (reduce index-triple index triples))

(defn insert-datoms [graph datoms]
  (let [triples (map #(datom/as-vec %) datoms)]
    (reduce (fn [graph index-type]
              (update graph index-type index-triples
                      (map (datom-reorder-fn index-type) triples)))
            graph index-types)))

(defn entity [{:keys [eav] :as graph} eid]
  (-> (get eav eid)
      (update-vals first)))

(comment
  (def datoms (->> (range 100)
                   (partition 5)
                   (map #(apply datom/->Datom %))
                   (take 3)))

  (def g (insert-datoms (memory-graph) datoms))

  (def retraction (datom/->Datom 0 1 2 nil false))

  (insert-datoms g [retraction])
  )
