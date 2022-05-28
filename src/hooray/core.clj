(ns hooray.core
  (:require [hooray.db :as db]
            [hooray.query :as query]))

(defn q [query & inputs]
  {:pre [(> (count inputs) 1)]}
  (query/query query inputs))
