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
            [hooray.db.persistent.packing :as pack]
            [hooray.db.persistent.redis :as redis])
  (:import (hooray.db.memory.graph_index SimpleIterator LeapIteratorCore LeapIteratorAVL)))

(t/use-fixtures :each fix/with-redis-keyspace)

(comment
  (redis/clear-set fix/redis-conn :eav))

;; TODO fix redis delay problem or figure out how to deal with it
(deftest redis-iterator-test
  (let [data [[1 1 2] [1 2 3] [1 2 4] [1 2 5] [1 3 4]]
        packed (map #(apply pack/pack-hash-array %) data)
        _ (proto/set-ks fix/*key-store* :eav #_fix/*keyspace* packed)
        it (per-g/->redis-iterator {:triple [1 2 '?v] :triple-order [:e :a :v]} fix/*key-store*)]
    #_(proto/set-ks fix/*key-store* :eav #_fix/*keyspace* packed)
    ;; (is (= 'data (proto/seek fix/*key-store* :eav (pack/pack-hash-array 1 2))))
    (is (false? (itr/at-end? it)))
    (is (= 3 (itr/key it)))
    (is (= 4 (-> it itr/next itr/key)))
    (is (= 5 (-> it itr/next itr/next itr/key)))
    (is (= 5 (-> it itr/next itr/next itr/key)))
    (is (nil? (-> it itr/next itr/next itr/next itr/key)))
    (redis/clear-set fix/redis-conn :eav)))

(deftest redis-iterator-seek-test
  (let [data (concat [[1 1 2]]
                     (for [i (range 10000)]
                       [1 2 i])
                     [[1 3 4]])
        packed (map #(apply pack/pack-hash-array %) data)
        _ (proto/set-ks fix/*key-store* :eav packed)
        it (per-g/->redis-iterator {:triple [1 2 '?v] :triple-order [:e :a :v]} fix/*key-store*)]
    #_(proto/set-ks fix/*key-store* :eav #_fix/*keyspace* packed)
    ;; (is (= 'data (proto/seek fix/*key-store* :eav (pack/pack-hash-array 1 2))))
    (is (false? (itr/at-end? it)))
    (is (= 0 (itr/key it)))
    (is (= 1 (-> it itr/next itr/key)))
    (is (= 2 (-> it itr/next itr/next itr/key)))
    (is (= 1000 (-> it (itr/seek 1000) itr/key)))
    (is (= 1001 (-> it (itr/seek 1000) itr/next itr/key)))
    (is (= 9999 (-> it (itr/seek 9999) itr/key)))
    (is (nil? (-> it (itr/seek 9999) itr/next itr/key)))
    (is (false? (-> it (itr/seek 9999) itr/at-end?)))
    (is (true? (-> it (itr/seek 9999) itr/next itr/at-end?)))
    (is (nil? (-> it (itr/seek 10000) itr/key)))
    (is (true? (-> it (itr/seek 10000) itr/at-end?)))
    (redis/clear-set fix/redis-conn :eav)))
