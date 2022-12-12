(ns hooray.db.persistent.fdb
  (:require [me.vedang.clj-fdb.FDB :as cfdb]
            [me.vedang.clj-fdb.core :as fc]
            [me.vedang.clj-fdb.impl :as fimpl]
            [me.vedang.clj-fdb.subspace.subspace :as fsub]
            [me.vedang.clj-fdb.transaction :as ftr]
            [me.vedang.clj-fdb.tuple.tuple :as ftub]
            [me.vedang.clj-fdb.range :as frange]
            [taoensso.nippy :as nippy]))

(def fdb (cfdb/select-api-version cfdb/clj-fdb-api-version))
(def db (cfdb/open fdb))

(defn ->buffer [v] (nippy/freeze v))
(defn ->value [b] (nippy/thaw b))

(defn set-k [db keyspace k]
  (let [subspace (fsub/create [keyspace])]
    (fc/set db subspace k nil)))

(defn set-ks [db keyspace ks]
  (let [subspace (fsub/create [keyspace])]
    (ftr/run db
      (fn [tr]
        (doseq [k ks]
          (fc/set tr subspace k nil))))))

(defn delete-k [db keyspace k]
  (let [subspace (fsub/create [keyspace])]
    (fc/clear db subspace k)))

(defn clear-ks [db keyspace ks]
  (let [subspace (fsub/create [keyspace])]
    (ftr/run db
      (fn [tr]
        (doseq [k ks]
          (fc/clear tr subspace k))))))

(defn get-k [db keyspace k]
  (let [subspace (fsub/create [keyspace])]
    (when (fc/get db subspace k) k)))

(defn- ->byte-array [ba1 ba2]
  (byte-array (mapcat seq [ba1 ba2])))



(defn get-range
  ([db keyspace start-k stop-k]
   (throw (ex-info "Not yet implemented! Missing `stop-k` option." {}))
   #_(fc/get-range db subspace (fc/range start-k stop-k)))
  ([db keyspace start-k stop-k limit]
   (throw (ex-info "Not yet implemented! Missing `stop-k` option." {}))))

(defn seek
  ([db keyspace prefix-k]
   (let [subspace (fsub/create [keyspace])]
     (fc/get-range db (fsub/create (->byte-array (fsub/pack subspace) prefix-k)))
     #_(fc/get-range db (frange/starts-with (->byte-array (fsub/pack subspace) prefix-k)))
     (fc/get-range db (frange/starts-with (->byte-array (fsub/pack subspace) prefix-k)))
     (fc/get-range db (frange/starts-with (fimpl/encode subspace prefix-k) #_(->byte-array (fsub/pack subspace) prefix-k)))
     (fc/get-range db (frange/starts-with (fimpl/encode subspace prefix-k)))
     #_(fc/get-range db subspace prefix-k)))
  ([conn keyspace prefix-k limit]
   (throw (ex-info "Not yet implemented! Missing `limit` option." {}))))

(comment
  (set-k db "store" (->buffer "foo"))
  (->value (get-k db "store" (->buffer "foo")))
  (get-k db "store" (->buffer "dafdsa"))
  (delete-k db "store" (->buffer "foo"))

  (->> (for [i (range 10)]
         (str "foo" i))
       (map ->buffer)
       (set-ks db "store"))

  (def subspace (fsub/create ["store"]))
  (def ^:private empty-byte-array (byte-array 0))

  (-> (fc/get-range db subspace)
      (update-keys (fn [v] (map ->value v))))

  (fc/get-range db subspace empty-byte-array)
  (fc/get-range db subspace (->buffer "foo"))

  (def prefix-k (->buffer "foo"))
  (fc/get-range db (frange/starts-with (fimpl/encode subspace prefix-k)))
  (subspace prefix-k)

  (->> (seek db "store" (->buffer "fo0"))
       #_(map #(try (->value %) (catch Exception e :some-error))))

  )



;; Set a value in the DB.
(let [fdb (cfdb/select-api-version cfdb/clj-fdb-api-version)]
  (with-open [db (cfdb/open fdb)]
    (fc/set db ["a" "test" "key"] ["some value"])))
;; => nil

;; Read this value back in the DB.
(let [fdb (cfdb/select-api-version cfdb/clj-fdb-api-version)]
  (with-open [db (cfdb/open fdb)]
    (fc/get db ["a" "test" "key"])))
;; => ["some value"]

;; FDB's Tuple Layer is super handy for efficient range reads. Each
;; element of the tuple can act as a prefix (from left to right).
(let [fdb (cfdb/select-api-version cfdb/clj-fdb-api-version)]
  (with-open [db (cfdb/open fdb)]
    (fc/set db ["test" "keys" "A"] ["value A"])
    (fc/set db ["test" "keys" "B"] ["value B"])
    (fc/set db ["test" "keys" "C"] ["value C"])
    (fc/get-range db ["test" "keys"])))
;; => {["test" "keys" "A"] ["value A"],
;;     ["test" "keys" "B"] ["value B"],
;;     ["test" "keys" "C"] ["value C"]}

;; FDB's Subspace Layer provides a neat way to logically namespace keys.
(let [fdb (cfdb/select-api-version cfdb/clj-fdb-api-version)
      subspace (fsub/create ["test" "keys"])]
  (with-open [db (cfdb/open fdb)]
    (fc/set db subspace ["A"] ["Value A"])
    (fc/set db subspace ["B"] ["Value B"])
    (fc/get db subspace ["A"])))
;; => ["Value A"]

(let [fdb (cfdb/select-api-version cfdb/clj-fdb-api-version)
      subspace (fsub/create ["test" "keys"])]
  (with-open [db (cfdb/open fdb)]
    (fc/set db subspace ["A"] ["Value A"])
    (fc/set db subspace ["B"] ["Value B"])
    (fc/get-range db subspace [] {:valfn first})))
;; => {["A"] "Value A", ["B"] "Value B"}

(let [fdb (cfdb/select-api-version cfdb/clj-fdb-api-version)
      subspace (fsub/create ["test" "keys"])]
  (with-open [db (cfdb/open fdb)]
    (fc/set db subspace ["A"] ["Value A"])
    (fc/set db subspace ["B"] ["Value B"])
    (fc/get-range db subspace [])))
;; => {["A"] ["Value A"], ["B"] ["Value B"], ["C"] ["value C"]}

;; FDB's functions are beautifully composable. So you needn't
;; execute each step of the above function in independent
;; transactions. You can perform them all inside a single
;; transaction. (with the full power of ACID behind you)
(let [fdb (cfdb/select-api-version cfdb/clj-fdb-api-version)]
  (with-open [db (cfdb/open fdb)]
    (ftr/run db
      (fn [tr]
        (fc/set tr ["test" "keys" "A"] ["value inside transaction A"])
        (fc/set tr ["test" "keys" "B"] ["value inside transaction B"])
        (fc/set tr ["test" "keys" "C"] ["value inside transaction C"])
        (fc/get-range tr ["test" "keys"])))))
;; => {["test" "keys" "A"] ["value inside transaction A"],
;;     ["test" "keys" "B"] ["value inside transaction B"],
;;     ["test" "keys" "C"] ["value inside transaction C"]}

;; The beauty and power of this is here:
(let [fdb (cfdb/select-api-version cfdb/clj-fdb-api-version)]
  (with-open [db (cfdb/open fdb)]
    (try (ftr/run db
           (fn [tr]
             (fc/set tr ["test" "keys" "A"] ["NEW value A"])
             (fc/set tr ["test" "keys" "B"] ["NEW value B"])
             (fc/set tr ["test" "keys" "C"] ["NEW value C"])
             (throw (ex-info "I don't like completing transactions"
                             {:boo :hoo}))))
         (catch Exception _
           (fc/get-range db ["test" "keys"])))))
;; => {["test" "keys" "A"] ["value inside transaction A"],
;;     ["test" "keys" "B"] ["value inside transaction B"],
;;     ["test" "keys" "C"] ["value inside transaction C"]}
;; No change to the values because the transaction did not succeed!

;; I hope this helps you get started with using this library!
