(ns scratch
  "Mainly testing out things and to explore other dbs."
  (:require [asami.core :as d]
            [xtdb.api :as xt]))

(def node (xt/start-node {}))
(xt/submit-tx node [[::xt/put {:xt/id 1 :data/foo 'bar}]])
(xt/q (xt/db node)
      '{:find [?e]
        :where [[?e :data/foo 'bar]]})
;; => #{}

(xt/q (xt/db node)
      '{:find [?e]
        :where [[?e :data/foo bar]
                [(clojure.core/= bar 'bar)]]})
;; => #{[1]}
