(ns hooray.db.persistent.graph-test
  (:refer-clojure :exclude [rand-int])
  (:require [clojure.data.avl :as avl]
            [clojure.tools.logging :as log]
            [clojure.edn :as edn]
            [clojure.spec.alpha :as s]
            [clojure.test :refer [deftest testing is] :as t]
            [hooray.db.memory.graph-index :as mem-gi]
            [hooray.fixtures :as fix]
            [hooray.graph :as g]
            [hooray.db.iterator :as itr]
            [hooray.query.spec :as h-spec]
            [hooray.util :as util :refer [dissoc-in]]
            [hooray.db.persistent.graph :as per-g]
            [hooray.db.persistent.protocols :as proto]
            [hooray.db.persistent.packing :as pack])
  (:import (hooray.db.memory.graph_index SimpleIterator LeapIteratorCore LeapIteratorAVL)))

(t/use-fixtures :each fix/with-redis-keyspace)

(deftest redis-iterator-test
  (let [data [[1 1 2] [1 2 3] [1 2 4] [1 2 5] [1 3 4]]
        packed (map #(apply pack/pack-hash-array %) data)]
    (println (type fix/*key-store*))
    (proto/set-ks fix/*key-store* fix/*keyspace* packed)
    (is (= data
           (->> (map #(proto/get-k fix/*key-store* fix/*keyspace* %) packed)
                (map #(pack/unpack-hash-array %)))))))
