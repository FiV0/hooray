(ns hooray.util.avl
  "Helpers for clojure.data.avl"
  (:refer-clojure :exclude [update-in])
  (:require [clojure.data.avl :as avl])
  (:import (clojure.data.avl AVLMap)))

(defn update-in
  "Like `clojure.core/update-in` but defaults to clojure.data.avl maps."
  ([m ks f & args]
   (let [up (fn up [m ks f args]
              (let [[k & ks] ks]
                (if ks
                  (assoc (if (nil? m) (avl/sorted-map) m) k (up (get m k) ks f args))
                  (assoc (if (nil? m) (avl/sorted-map) m) k (apply f (get m k) args)))))]
     (up (if (nil? m) (avl/sorted-map) m) ks f args))))

(comment (type (update-in nil [1 2 3] identity))
         (type (get (update-in nil [1 2 3] identity) 1))
         (type (get (get (update-in nil [1 2 3] identity) 1) 2)))

;; TODO move away from util ns
(defn create-update-in
  "Creates a function that behaves like `clojure.core/update-in` but defaults to `(default-map-fn)`
  for empty maps."
  [default-map-fn]
  (let [m-or-default (fn [m] (if (nil? m) (default-map-fn) m))]
    (fn custom-update-in [m ks f & args]
      (let [up (fn up [m ks f args]
                 (let [[k & ks] ks]
                   (if ks
                     (assoc (m-or-default m) k (up (get m k) ks f args))
                     (assoc (m-or-default m) k (apply f (get m k) args)))))]
        (up (m-or-default m) ks f args)))))

(comment
  (def m (clojure.core/update-in (sorted-map) [1 2] (fnil conj (sorted-set)) 3))
  (sorted? (m 1))

  (def sorted-update-in (create-update-in sorted-map))
  (def m (sorted-update-in (sorted-map) [1 2] (fnil conj (sorted-set)) 3))
  (sorted? (m 1)))
