(ns hooray.core
  (:require [hooray.db :as db]
            [hooray.db.memory]
            [hooray.db.persistent]
            [hooray.query :as query]))

(defn connect [uri]
  (db/connect uri))

(defn transact [conn tx-data]
  (db/transact conn tx-data))

(defn q [query & inputs]
  {:pre [(>= (count inputs) 1)]}
  (query/query query (first inputs)))

(comment
  (require '[clojure.edn :as edn])

  (def conn (db/connect "hooray:mem://data"))
  (def data (edn/read-string (slurp "resources/transactions.edn")))

  (transact conn data)

  (time (q '{:find [?name ?album]
             :where [[?t :track/name "For Those About To Rock (We Salute You)" ]
                     [?t :track/album ?album]
                     [?album :album/artist ?artist]
                     [?artist :artist/name ?name]]}
           (db/db conn)))


  )
