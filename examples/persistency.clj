(ns persistency
  (:require [hooray.core :as h]
            [hooray.graph-gen :as g-gen]))

(def config-map-redis {:type :per
                       :sub-type :redis
                       :name "hello"
                       :algo :leapfrog
                       :spec {:uri "redis://localhost:6379/"}})

(def redis-conn (h/connect config-map-redis))

(def random-graph (g-gen/random-graph 300 0.3))
(h/transact redis-conn (g-gen/graph->ops random-graph))

(def triangle-query '{:find [?a ?b ?c]
                      :where [[?a :g/to ?b]
                              [?a :g/to ?c]
                              [?b :g/to ?c]]})

(time (count (h/q triangle-query (h/db redis-conn))))
;; "Elapsed time: 22029.502057 msecs"
