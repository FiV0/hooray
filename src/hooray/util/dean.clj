(ns hooray.util.dean
  "Helpers for lean interval trees."
  (:refer-clojure :exclude [get-in update-in])
  (:import (com.dean.interval_tree.tree.interval_map IntervalMap)))

(defn update-in
  "Like `clojure.core/update-in` but also works for interval trees"
  [m ks f & args]
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
    (up m ks f args)))

(comment
  (require '[com.dean.interval-tree.core :as dean])

  (instance? IntervalMap (dean/interval-map))

  (update-in {} [:a :b] (fnil conj []) 1)
  (update-in (dean/interval-map) [[1 2] :b] (fnil conj []) 1)
  (-> (update-in (dean/interval-map {[1 2] (dean/interval-map)}) [[1 2] [2 3]] (fnil conj []) 1)
      (find [1 2])
      val
      type))

(defn get-in
  "Like `clojure.core/get-in` but also works for interval trees"
  ([m ks]
   (get-in m ks nil))
  ([m ks not-found]
   (loop [sentinel (Object.)
          m m
          ks (seq ks)]
     (if ks
       (if (instance? IntervalMap m)
         (if-let [m (some-> (find m (first ks)) val)]
           (recur sentinel m (next ks))
           not-found)
         (let [m (get m (first ks) sentinel)]
           (if (identical? sentinel m)
             not-found
             (recur sentinel m (next ks)))))
       m))))

(comment
  (-> (update-in (dean/interval-map) [[1 2] :b] (fnil conj []) 1)
      (get-in [[1 2] :b]))

  (-> {}
      (assoc :b (dean/interval-map {[1 2] [1]}))
      (get-in [:b [1 2]]))

  (-> {}
      (assoc :b (dean/interval-map {[1 2] [1]}))
      (get-in [[1 2] :b]))

  )

;; TODO assoc-in
