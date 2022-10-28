(ns hooray.algo.hash-join
  (:require [hooray.graph :as g]
            [hooray.db :as db]))

;; binary join strategy on clauses

(defn create-lookup [pattern result var->binding]
  (let [size (count result)]
    (mapv (fn [v]
            (if-let [index (get var->binding v)]
              (if (< index size) ;; o/w var not yet bound
                (nth result index)
                v)
              v))
          pattern)))

(defn left-join [pattern results graph var->bindings]
  (for [lrow results
        :let [lookup (create-lookup pattern lrow var->bindings)]
        rrow (g/resolve-triple graph lookup)]
    (concat lrow rrow)))

(defn join [{:keys [query var->bindings] :as _compiled-q} db]
  (if-let [where (:where query)]
    (if (seq where)
      (let [graph (db/graph db)
            first-results (g/resolve-triple graph (first where))
            ljoin #(left-join %2 %1 graph var->bindings)]
        (reduce ljoin first-results (rest where)))
      (throw (ex-info "Where can't be empty!" {:where where})))
    (throw (ex-info "Query must contain where clause!" {:query query}))))
