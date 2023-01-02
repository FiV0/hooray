(ns hooray.core
  (:require [clojure.tools.logging :as log]
            [hooray.db :as db]
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
  (when (> (count inputs) 1)
    (log/warn "Hooray currently only supports one source!"))
  (query/query query (first inputs)))

(defn db [conn]
  #_{:pre [(instance? MemoryConnection conn)]}
  (db/db conn))

;;///////////////////////////////////////////////////////////////////////////////
;;===============================================================================
;;                                   in-momory
;;===============================================================================
;;\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\

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


;;///////////////////////////////////////////////////////////////////////////////
;;===============================================================================
;;                                     redis
;;===============================================================================
;;\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\

(comment
  (require '[clojure.edn :as edn]
           '[hooray.graph-gen :as g-gen])

  (def config-map-lf {:type :per
                      :sub-type :redis
                      :name "hello"
                      :algo :leapfrog
                      :spec {:uri "redis://localhost:6379/"}})

  (def config-map-hs {:type :per
                      :sub-type :redis
                      :name "hello"
                      :algo :hash
                      :spec {:uri "redis://localhost:6379/"}})

  (def redis-conn-lf (connect config-map-lf))
  (def redis-conn-hs (connect config-map-hs))
  (db/drop-db redis-conn-lf)
  (def data (edn/read-string (slurp "resources/transactions.edn")))
  (transact redis-conn-hs  [{:db/id 0
                             :hello :world}
                            [:db/add 1 :foo :bar]])
  (time (transact redis-conn-hs data))

  (def results (time (q '{:find [?e ?a ?v]
                          :where [[?e ?a ?v]]}
                        (db redis-conn-hs))))

  (time (q '{:find [?name ?album]
             :where [[?t :track/name "For Those About To Rock (We Salute You)" ]
                     [?t :track/album ?album]
                     [?album :album/artist ?artist]
                     [?artist :artist/name ?name]]}
           (db redis-conn-hs)))

  (def random-graph (g-gen/random-graph 10 0.3))
  (transact redis-conn-lf (g-gen/graph->ops random-graph))
  (transact conn (g-gen/graph->ops random-graph))

  (def triangle-query '{:find [?a ?b ?c]
                        :where [[?a :g/to ?b]
                                [?a :g/to ?c]
                                [?b :g/to ?c]]})

  (time (count (q triangle-query (db redis-conn-hs))))
  (time (count (q triangle-query (db redis-conn-lf))))
  (time (count (q triangle-query (db conn)))))
