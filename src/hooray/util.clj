(ns hooray.util
  (:require [clojure.edn :as edn]))

(defn read-edn [f]
  (-> (slurp f)
      edn/read-string))

(comment
  (read-edn "resources/transactions.edn")
  )

(defn now [] (java.util.Date.))

;; copied from clojure.incubator
(defn dissoc-in
  "Dissociates an entry from a nested associative structure returning a new
  nested structure. keys is a sequence of keys. Any empty maps that result
  will not be present in the new structure."
  [m [k & ks :as keys]]
  (if ks
    (if-let [nextmap (get m k)]
      (let [newmap (dissoc-in nextmap ks)]
        (if (seq newmap)
          (assoc m k newmap)
          (dissoc m k)))
      m)
    (dissoc m k)))

(comment
  (def a {:a {:b #{:c}}})
  (def b {:a {:b {:c :d} :foo {}} :d {}})
  (dissoc-in b [:a :b :c])

  )

(defn variable? [v]
  (and (symbol? v) (= \? (first (name v)))))

(defn wildcard? [v]
  (= v '_))

(defn constant? [v]
  (not (or (wildcard? v) (variable? v))))
