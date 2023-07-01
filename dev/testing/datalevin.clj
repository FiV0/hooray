(ns testing.datalevin
  (:require [datalevin.core :as d]
            [hooray.graph-gen :as g-gen]))

(comment
  (def schema {:g/to {:db/cardinality :db.cardinality/many}})
  (def conn (d/get-conn "/tmp/datalevin/test7" schema))

  (def the-graph (g-gen/complete-graph 100))

  (d/transact! conn (g-gen/graph->ops the-graph))

  (def triangle-query '{:find [?a ?b ?c]
                        :where [[?a :g/to ?b]
                                [?a :g/to ?c]
                                [?b :g/to ?c]]})

  (time (count (d/q triangle-query (d/db conn))))

  (def client (d/get-conn "dtlv://datalevin:datalevin@localhost/test3" schema))
  (d/transact! client (g-gen/graph->ops the-graph))
  (time (count (d/q triangle-query (d/db client)))))
