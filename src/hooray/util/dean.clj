(ns hooray.util.dean
  "Helpers for lean interval trees."
  (:refer-clojure :exclude [get-in update-in])
  (:import (com.dean.interval_tree.tree.interval_map IntervalMap)))

(defn update-in
  "Like `clojure.core/update-in` but also works for interval trees"
  ([m ks f & args]
   (let [up (fn up [m ks f args]
              (let [[k & ks] ks]
                (if (instance? IntervalMap m)
                  (let [old (some-> (find m k) val)]
                    (if ks
                      (assoc m k (up old ks f args))
                      (assoc m k (apply f old args))))
                  (if ks
                    (assoc m k (up (get m k) ks f args))
                    (assoc m k (apply f (get m k) args))))))]
     (up m ks f args))))

(comment
  (require '[com.dean.interval-tree.core :as dean])

  (instance? IntervalMap (dean/interval-map))

  (update-in {} [:a :b] (fnil conj []) 1)
  (update-in (dean/interval-map) [[1 2] :b] (fnil conj []) 1)
  (-> (update-in (dean/interval-map {[1 2] (dean/interval-map)}) [[1 2] [2 3]] (fnil conj []) 1)
      (find [1 2])
      val
      type))

(defn get-in)
