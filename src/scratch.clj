(ns scratch
  "Mainly testing out things and to explore other dbs."
  (:require [asami.core :as d]
            [xtdb.api :as xt]))

(def node (xt/start-node {}))
