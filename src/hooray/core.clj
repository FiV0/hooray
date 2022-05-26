(ns hooray.core
  (:require [hooray.db :as db]
            [hooray.querying :as query]))

(defn q [query & inputs]
  {:pre [(> (count inputs) 1)]}
  (query/q query (first inputs)))
