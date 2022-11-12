(ns hooray.core
  (:require [hooray.db :as db]
            [hooray.db.memory]
            [hooray.db.persistent]
            [hooray.query :as query])
  (:import (hooray.db.memory MemoryConnection)))

(defn connect [uri]
  (db/connect uri))

(defn transact [conn tx-data]
  (db/transact conn tx-data))

(defn q [query & inputs]
  {:pre [(>= (count inputs) 1)]}
  (query/query query (first inputs)))

(defn db [conn]
  {:pre [(instance? MemoryConnection conn)]}
  (db/db conn))

(comment
  (require '[clojure.edn :as edn]
           '[hooray.util :as util])

  (def data (edn/read-string (slurp "resources/transactions.edn")))

  (do
    (def conn (connect "hooray:mem://data"))
    (def conn-core (connect "hooray:mem:core//data"))
    (def conn-avl (connect "hooray:mem:avl//data"))
    (def conn-tonsky (connect "hooray:mem:tonsky//data"))
    (def conn-avl-generic (connect "hooray:mem:avl:generic//data"))

    (transact conn data)
    (transact conn-core data)
    (transact conn-avl data)
    (transact conn-tonsky data)
    (transact conn-avl-generic data)

    (defn db-bin [] (db conn))
    (defn db-core [] (db conn-core))
    (defn db-avl [] (db conn-avl))
    (defn db-tonsky [] (db conn-tonsky))
    (defn db-generic [] (db conn-avl-generic)))

  (for [db-fn [db-bin db-core db-avl db-tonsky db-generic]]
    (util/with-timing
      (q '{:find [?name ?album]
           :where [[?t :track/name "For Those About To Rock (We Salute You)" ]
                   [?t :track/album ?album]
                   [?album :album/artist ?artist]
                   [?artist :artist/name ?name]]}
         (db-fn))))

  (time (q '{:find [?track-name ?album-title]
             :where [[?artist :artist/name "Ozzy Osbourne"]
                     [?album :album/artist ?artist]
                     [?album :album/title ?album-title]
                     [?t :track/album ?album]
                     [?t :track/name ?track-name]]}
           (db-generic))))
