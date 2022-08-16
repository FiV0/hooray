(ns hooray.testing.xt
  (:require [xtdb.api :as xt]
            [clojure.edn :as edn]))

(defn wrap-in-puts [data]
  (map #(vector ::xt/put %) data))

(def data (->> (edn/read-string (slurp "resources/transactions.edn"))
               (map #(clojure.set/rename-keys % {:db/id :xt/id}))
               (wrap-in-puts)))

(def node (xt/start-node {}))

(xt/submit-tx node data)

(defn db [] (xt/db node))

(xt/q (db)
      '{:find [?name]
        :where
        [[?t :track/name "For Those About To Rock (We Salute You)"]
         [?t :track/album ?album]
         [?album :album/artist ?artist]
         [?artist :artist/name ?name]]})

(comment
  (require '[xtdb.query])
  (sc.api/letsc [1 -1]
                conformed-query)

  )
