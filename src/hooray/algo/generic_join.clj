(ns hooray.algo.generic-join
  (:refer-clojure :exclude [count extend])
  (:require [hooray.db :as db]))

;; generic-join
;; based on
;; http://www.frankmcsherry.org/dataflow/relational/join/2015/04/11/genericjoin.html
;; https://arxiv.org/abs/1310.3314

(defprotocol ISeek
  (seek
    [this k]
    [this k cmp]))

(extend-protocol ISeek
  clojure.lang.PersistentVector
  (seek [this k] (throw (ex-info "toto" {})))
  (seek [this k cmp] (throw (ex-info "toto" {}))) )

(defprotocol PrefixExtender
  (count [this prefix])
  (propose [this prefix])
  (intersect [this prefix extensions]))

(defrecord PatternPrefixExtender [pattern var-join-order graph])

;; prefixes are partial rows
(defn extend-prefix [extenders prefix]
  {:pre [(assert (> (clojure.core/count extenders) 0))]}
  (let [[extender _] (->> (map #(vector % (count % prefix)) extenders)
                          (reduce (fn [[_e1 c1 :as r1] [_e2 c2 :as r2]] (if (> c1 c2) r1 r2)) extenders))
        remaining (remove #{extender} extenders)
        extensions (propose extender prefix)
        extensions (reduce #(intersect %2 prefix %1) extensions remaining)]
    (map #(conj prefix %) extensions)))

(defn extend [extenders prefixes]
  (mapcat #(extend-prefix extenders %) prefixes))

(defn join [{:keys [conformed-query var-join-order var->bindings] :as _compiled-q} db]
  {:pre [(vector? var-join-order)]}
  (if-let [where (:where conformed-query)]
    (let [max-level (count var-join-order)
          graph (db/graph db)
          ]
      )
    (throw (ex-info "Query must contain where clause!" {:query conformed-query}))))
