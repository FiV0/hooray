(ns hooray.algo.hash-join
  (:require [hooray.db :as db]
            [hooray.graph :as g]
            [hooray.util :as util]))

;; hash join strategy on clauses
(defn create-lookup [pattern result var->binding]
  (let [size (count result)]
    (mapv (fn [v]
            (if-let [index (get var->binding v)]
              (if (< index size) ;; o/w var not yet bound
                (nth result index)
                v)
              v))
          pattern)))

(defn remove-duplicates [lookup row]
  (let [[a b c] (remove util/constant? lookup)]
    (cond (nil? b) row
          (nil? c) (if (= a b)
                     (take 1 row)
                     row)
          :else (cond (= a b c) (take 1 row)
                      (= a b) (list (first row) (nth row 2))
                      (or (= a c) (= b c)) (take 2 row)
                      :else row))))

(defn left-join [pattern results graph var->bindings]
  (for [lrow results
        :let [lookup (create-lookup pattern lrow var->bindings)]
        rrow (->> (g/resolve-triple graph lookup)
                  (remove-duplicates lookup))]
    (concat lrow rrow)))

(defn join [{:keys [query var->bindings] :as _compiled-q} db]
  (if-let [where (:where query)]
    (if (seq where)
      (let [graph (db/graph db)
            first-pattern (first where)
            first-results (->> (g/resolve-triple graph first-pattern)
                               (map #(remove-duplicates first-pattern %)))
            ljoin #(left-join %2 %1 graph var->bindings)]
        (reduce ljoin first-results (rest where)))
      (throw (ex-info "Where can't be empty!" {:where where})))
    (throw (ex-info "Query must contain where clause!" {:query query}))))
