(ns hooray.testing.xt
  (:require [xtdb.api :as xt]
            [xtdb.query :as query]
            [clojure.edn :as edn]))

(defn wrap-in-puts [data]
  (map #(vector ::xt/put %) data))

(def data (->> (edn/read-string (slurp "resources/transactions.edn"))
               (map #(clojure.set/rename-keys % {:db/id :xt/id}))
               (wrap-in-puts)))

(def node (xt/start-node {}))
(.close node)

(xt/submit-tx node data)

(defn db [] (xt/db node))

(xt/q (db)
      '{:find [?name]
        :where
        [[?t :track/name "For Those About To Rock (We Salute You)"]
         [?t :track/album ?album]
         [?album :album/artist ?artist]
         [?artist :artist/name ?name]]})

;;;;;;;;;;;;;;;;;
;; triangle query
;;;;;;;;;;;;;;;;;

(require '[hooray.graph-gen :as g-gen])

(def random-graph (clojure.edn/read-string (slurp "resources/random-graph-100-0.3.edn")))
(def random-independents (clojure.edn/read-string (slurp "resources/random-independents-graph-100-0.3.edn")))

(defn graph->ops [g]
  (->> (g-gen/edge-list->adj-list g)
       (map (fn [[from adj-list]] {:xt/id from :g/to (into #{} adj-list)}))))

(def random-graph-data (graph->ops random-graph))
(def random-independents-graph-data (graph->ops random-independents))

(xt/submit-tx node (wrap-in-puts random-graph-data))
(xt/submit-tx node (wrap-in-puts random-independents-graph-data))

(def triangle-query '{:find [?a ?b ?c]
                      :where [[?a :g/to ?b]
                              [?a :g/to ?c]
                              [?b :g/to ?c]]})

(time (count (xt/q (db) triangle-query)))
