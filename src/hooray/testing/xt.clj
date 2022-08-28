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


(query/query-plan-for (db) '{:find [?name]
                             :where
                             [[?t :track/name "Foo"]
                              [?t :track/album "Foo"]]})


(comment
  (require '[xtdb.query])
  (sc.api/letsc [1 -1]
                var->joins)

  (def n (xt/start-node {}))
  (xt/submit-tx n [[::xt/put {:xt/id 1 :data/foo 'foo}]])

  (xt/q (xt/db n)
        '{:find [?e]
          :where [[?e :data/foo foo]
                  [(clojure.core/= foo 'foo)]]}
        )

  )
