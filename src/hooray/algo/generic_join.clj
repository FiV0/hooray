(ns hooray.algo.generic-join
  (:refer-clojure :exclude [count extend])
  (:require [clojure.data.avl :as avl]
            [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [hooray.db :as db]
            [hooray.db.memory.graph-index :as g-index]
            [hooray.graph :as graph]
            [hooray.util :as util]
            [hooray.util.persistent-map :as tonsky-map]))

;; generic-join based on
;; http://www.frankmcsherry.org/dataflow/relational/join/2015/04/11/genericjoin.html
;; https://arxiv.org/abs/1310.3314

(defprotocol PrefixExtender
  (count [this prefix])
  (propose [this prefix])
  (intersect [this prefix extensions]))

(s/def ::prefix (s/coll-of integer? :kind vector?))

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

(defn- next-var-index [pattern var]
  (cond
    (= (nth pattern 0) var) 0
    (= (nth pattern 1) var) 1
    (= (nth pattern 2) var) 2
    :else (throw (ex-info "Var not in pattern!" {:var var :pattern var}))))

(def idx->name {0 :e 1 :a 2 :v})

(defn- pos->literal [var? var->bindings prefix]
  (cond (util/constant? var?) (g-index/hash var?)
        (< (var->bindings var?) (clojure.core/count prefix))
        (nth prefix (var->bindings var?))
        :else nil))

(defn prefix->tuple-fn [pattern var-join-order]
  (let [var->bindings (zipmap var-join-order (range))]
    (fn prefix->tuple [prefix]
      (let [size (clojure.core/count prefix)
            next-var (nth var-join-order size)
            next-var-idx (next-var-index pattern next-var)
            [i j] (vec (set/difference #{0 1 2} #{next-var-idx}))
            i-literal (pos->literal (nth pattern i) var->bindings prefix)
            j-literal (pos->literal (nth pattern j) var->bindings prefix)]
        (cond-> {:triple-order [] :triple []}

          i-literal
          (-> (update :triple-order conj (idx->name i))
              (update :triple conj i-literal))

          j-literal
          (-> (update :triple-order conj (idx->name j))
              (update :triple conj j-literal))

          :always
          (-> (update :triple-order conj (idx->name next-var-idx))
              (update :triple conj next-var)))))))

(defrecord PatternPrefixExtender [pattern var-join-order graph resolve-tuple-fn prefix->tuple-fn seek-fn]
  PrefixExtender
  (count [_this prefix]
    (clojure.core/count (resolve-tuple-fn graph (prefix->tuple-fn prefix))))

  (propose [_this prefix]
    (let [index (resolve-tuple-fn graph (prefix->tuple-fn prefix))]
      (cond
        (set? index) (into [] (seq index))
        (map? index) (into [] (keys index))
        (nil? index) []
        :else (throw (ex-info "No propose op for this index type!" {:index-type (type index)})))))

  (intersect [_this prefix extensions]
    (let [index (resolve-tuple-fn graph (prefix->tuple-fn prefix))
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
                           (seek-fn s first-ext))
                    (> first-s first-ext)
                    (recur res
                           (binary-search extensions first-s pos)
                           s)))
            res))
        extensions))))

(defn type->seek-fn [type]
  (case type
    :avl avl/seek
    :tonsky tonsky-map/seek
    (throw (ex-info "Seek not supported on type!" {:type type}))))

(defn- ->pattern-prefix-extender [pattern var-join-order graph {:keys [sub-type] :as _uri-map}]
  (->PatternPrefixExtender pattern var-join-order graph
                           (memoize graph/resolve-tuple)
                           (memoize (prefix->tuple-fn pattern var-join-order))
                           (type->seek-fn sub-type)))

;; prefixes are partial rows
(defn extend-prefix [extenders prefix]
  {:pre [(> (clojure.core/count extenders) 0)]}
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

(defn join [{:keys [conformed-query query var-join-order _var->bindings] :as _compiled-q} db]
  {:pre [(vector? var-join-order)]}
  (if-let [where (:where query)]
    (let [graph (db/graph db)
          uri-map (-> db :opts :uri-map)
          var->extenders
          (reduce (fn [v->e clause]
                    (let [vars (filter util/variable? clause)]
                      (reduce #(update %1 %2 (fnil conj []) (->pattern-prefix-extender clause var-join-order graph uri-map))
                              v->e vars)))
                  {} where)]
      (loop [res '([]) level 0]
        (if (= level (clojure.core/count var-join-order))
          (map (partial lookup-row graph) res)
          (recur (extend (var->extenders (nth var-join-order level)) res) (inc level)))))
    (throw (ex-info "Query must contain where clause!" {:query conformed-query}))))
