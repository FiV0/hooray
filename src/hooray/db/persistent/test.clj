(ns test
  (:require [me.vedang.clj-fdb.FDB :as cfdb]
            [me.vedang.clj-fdb.core :as fc]
            [me.vedang.clj-fdb.impl :as fimpl]
            [me.vedang.clj-fdb.subspace.subspace :as fsub]
            [me.vedang.clj-fdb.range :as frange]
            [taoensso.nippy :as nippy]))

(def fdb (cfdb/select-api-version cfdb/clj-fdb-api-version))
(def db (cfdb/open fdb))

(defn ->buffer [v] (nippy/freeze v))
(defn ->value [b] (nippy/thaw b))

(def subspace (fsub/create ["the-store"]))

;; works fine
(fc/set db subspace (->buffer "foo") nil)
(fc/get db subspace (->buffer "foo"))
;; => []
(fc/get db subspace (->buffer "not present"))
;; => nil

;; put some more data in
(->> (for [i (range 10)]
       (str "foo" i))
     (map ->buffer)
     (map #(fc/set db subspace % nil)))

;; with only subspace works fine
(-> (fc/get-range db subspace)
    (update-keys (fn [v] (map ->value v))))
;; => {("foo8") [],
;;      ...
;;      ...
;;     ("foo4") []}

(def ^:private empty-byte-array (byte-array 0))
(fc/get-range db subspace empty-byte-array)
;; expects Tuple
(fc/get-range db subspace (->buffer "foo"))
;; expects Tuple

;; this gives me only the ["the-store" "foo"] prefix
(def prefix (->buffer "foo"))
(-> (fc/get-range db (frange/starts-with (fimpl/encode subspace prefix)))
    (update-keys (fn [v] (update v 1 ->value))))
;; => {["the-store" "foo"] []}

;; but I would like something of the sort
(fc/get-range db subspace (->buffer "foo"))
;; => {["foo"] [] , ["foo0"] [], ["foo1"] [] }
