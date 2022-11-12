(ns hooray.algo.generic-join
  (:refer-clojure :exclude [count extend])
  (:require [clojure.core.match :refer [match]]
            [clojure.data.avl :as avl]
            [clojure.spec.alpha :as s]
            [hooray.db :as db]
            [hooray.db.memory.graph-index :as g-index]
            [hooray.graph :as graph]
            [hooray.util :as util]))

;; generic-join
;; based on
;; http://www.frankmcsherry.org/dataflow/relational/join/2015/04/11/genericjoin.html
;; https://arxiv.org/abs/1310.3314

(defprotocol PrefixExtender
  (count [this prefix])
  (propose [this prefix])
  (intersect [this prefix extensions]))

(s/def ::prefix (s/coll-of integer? :kind vector?))

(defn prefix->tuple [prefix pattern var-join-order]
  {:pre [(< (clojure.core/count prefix) (clojure.core/count var-join-order))
         #_(s/valid? ::prefix prefix)]}
  (let [var->bindings (zipmap var-join-order (range))
        size (clojure.core/count prefix)
        next-var (nth var-join-order size)]
    (match (mapv (fn [v] (cond (and (util/variable? v) (< (var->bindings v) size))
                               [(nth prefix (var->bindings v)) true]

                               (util/constant? v)
                               [(g-index/hash v) true]

                               :else
                               [v false])) pattern)
      [[next-var _] [a true] [v true]] {:triple [a v next-var] :triple-order [:a :v :e]}
      [[next-var _] [_ false] [v true]] {:triple [v next-var] :triple-order [:v :e]}
      [[next-var _] [a true] [_ false]] {:triple [a next-var] :triple-order [:a :e]}
      [[next-var _] [_ false] [_ false]] {:triple [next-var] :triple-order [:e]}
      [[e true] [next-var _] [v true]] {:triple [e v next-var] :triple-order [:e :v :a]}
      [[_ false] [next-var _] [v true]] {:triple [v next-var] :triple-order [:v :a]}
      [[e true] [next-var _] [_ false]] {:triple [e next-var] :triple-order [:e :a]}
      [[_ false] [next-var _] [_ false]] {:triple [next-var] :triple-order [:a]}
      [[e true] [a true] [next-var _]] {:triple [e a next-var] :triple-order [:e :a :v]}
      [[_ false] [a true] [next-var _]] {:triple [a next-var] :triple-order [:a :v]}
      [[e true] [_ false] [next-var _]] {:triple [e next-var] :triple-order [:e :v]}
      [[_ false] [_ false] [next-var _]] {:triple [next-var] :triple-order [:v]}
      :else (throw (ex-info "prefix->tuple bad pattern!" {:pattern pattern})))))

(comment
  (prefix->tuple [1 2] '[?e :foo ?v] '[?e ?bla ?v]))

;; TOOO to adapt if remove doc-store for in-memory db
(defn- binary-search
  ([extensions k] (binary-search extensions k 0))
  ([extensions k lower-bound]
   (loop [l lower-bound r (clojure.core/count extensions)]
     (if (< l r)
       (let [m (quot (+ l r) 2)]
         (if (< (nth extensions m) k)
           (recur (inc m) r)
           (recur l m)))
       l))))

(comment
  (binary-search [1 2 3 5 8 10] 4)
  (binary-search [1 2 3 5 8 10] 11)
  (binary-search [1 2 3 5 8 10] 11 3)
  (binary-search [0 1] 1)
  (binary-search [0 1] 0)
  (binary-search [] 12))


(defrecord PatternPrefixExtender [pattern var-join-order graph]
  PrefixExtender
  (count [this prefix]
    (clojure.core/count (graph/resolve-tuple graph (prefix->tuple prefix pattern var-join-order))))

  (propose [this prefix]
    (let [index (graph/resolve-tuple graph (prefix->tuple prefix pattern var-join-order))]
      (cond (set? index) (into [] (seq index))
            (map? index) (into [] (keys index))
            :else (throw (ex-info "No propose op for this index type!" {:index-type (type index)})))))

  (intersect [this prefix extensions]
    (let [index (graph/resolve-tuple graph (prefix->tuple prefix pattern var-join-order))
          first-index (if (set? index) first ffirst)
          nb-exts (clojure.core/count extensions)]
      (if (seq extensions)
        (loop [res [] pos 0 s (seq index)]
          (if (and (seq s) (< pos nb-exts))
            (let [first-ext (nth extensions pos)
                  first-s (first-index s)]
              (cond (= first-s first-ext)
                    (recur (conj res first-s)
                           (inc pos)
                           (next s))
                    (< first-s first-ext)
                    (recur res
                           pos
                           (avl/seek s first-ext))
                    (> first-s first-ext)
                    (recur res
                           (binary-search extensions first-s pos)
                           s)))
            res))
        extensions))))

(defn- ->pattern-prefix-extender [pattern var-join-order graph]
  (->PatternPrefixExtender pattern var-join-order graph))

;; prefixes are partial rows
(defn extend-prefix [extenders prefix]
  {:pre [(> (clojure.core/count extenders) 0) "Need at least 1 extender!"]}
  (let [[extender _] (->> (map #(vector % (count % prefix)) extenders)
                          (reduce (fn [[_e1 c1 :as r1] [_e2 c2 :as r2]] (if (> c1 c2) r1 r2))))
        remaining (remove #{extender} extenders)
        extensions (propose extender prefix)
        extensions (reduce #(intersect %2 prefix %1) extensions remaining)]
    (map #(conj prefix %) extensions)))

(defn extend [extenders prefixes]
  (mapcat #(extend-prefix extenders %) prefixes))

(defn- lookup-row [graph row]
  (mapv #(g-index/hash->value graph %) row))

(defn join [{:keys [conformed-query query var-join-order var->bindings] :as _compiled-q} db]
  {:pre [(vector? var-join-order)]}
  (if-let [where (:where query)]
    (let [graph (db/graph db)
          var->extenders
          (reduce (fn [v->e clause]
                    (let [vars (filter util/variable? clause)]
                      (reduce #(update %1 %2 (fnil conj []) (->pattern-prefix-extender clause var-join-order graph)) v->e vars)))
                  {} where)]
      (loop [res '([]) level 0]
        (if (= level (clojure.core/count var-join-order))
          (map (partial lookup-row graph) res)
          (recur (extend (var->extenders (nth var-join-order level)) res) (inc level)))))
    (throw (ex-info "Query must contain where clause!" {:query conformed-query}))))
