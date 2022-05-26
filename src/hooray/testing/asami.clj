(ns hooray.testing.asami
  (:require [clojure.edn :as edn]
            [asami.core :as d]))

(def data (edn/read-string (slurp "resources/transactions.edn")))
(def conn (d/connect "asami:mem://data"))
(d/transact conn {:tx-data data})

(def data2 [{:db/id :a/node-mynode :name "Jane" :home "Longbourn"}])
(d/transact conn {:xt-data data2})


(d/q '[:find ?album #_?artist
       :where
       [?t :track/name "For Those About To Rock (We Salute You)" ]
       [?t :track/album ?album]
       ;; [?album :album/artist ?artist]
       ;; [?artist :artist/name ?name]
       ]
     #_'[:find ?t
         :where
         [?t "format" "image/jpeg"]
         [?image "height" ?height]
         [?image "width" ?width]]
     conn)
