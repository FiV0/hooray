(ns hooray.db.persistent.fdb
  (:require [me.vedang.clj-fdb.FDB :as cfdb]
            [me.vedang.clj-fdb.core :as fc]
            [me.vedang.clj-fdb.impl :as fimpl]
            [me.vedang.clj-fdb.subspace.subspace :as fsub]
            [me.vedang.clj-fdb.transaction :as ftr]
            [me.vedang.clj-fdb.tuple.tuple :as ftub]
            [me.vedang.clj-fdb.range :as frange]
            [me.vedang.clj-fdb.key-selector :as key-selector]
            [taoensso.nippy :as nippy]))

;; TODO/TO consider maybe use the tuple model directly for the indices

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

;; TODO figure out how to get rid of the subspace in the keys
(defn get-range
  ([db keyspace start-k stop-k]
   (let [subspace (fsub/create [keyspace])]
     (->> (fc/get-range2 db
                         (key-selector/first-greater-or-equal (fsub/pack subspace start-k))
                         (key-selector/first-greater-or-equal (fsub/pack subspace stop-k))
                         #_{:keyfn #(update % 1 ->value)})
          (map (comp second first)))))
  ([db keyspace start-k stop-k limit]
   (let [subspace (fsub/create [keyspace])]
     (->> (fc/get-range2 db
                         (key-selector/first-greater-or-equal (fsub/pack subspace start-k))
                         (key-selector/first-greater-or-equal (fsub/pack subspace stop-k))
                         {:limit limit
                          #_#_:keyfn #(update % 1 ->value)})
          (map (comp second first))))))

(defn seek
  ([db keyspace prefix-k]
   (let [subspace (fsub/create [keyspace])]
     (->> (fc/get-range2 db
                         (key-selector/first-greater-or-equal (fsub/pack subspace prefix-k))
                         (key-selector/first-greater-than (.-end (fsub/range subspace)))
                         #_{:keyfn #(update % 1 ->value)})
          (map (comp second first)))))
  ([db keyspace prefix-k limit]
   (let [subspace (fsub/create [keyspace])]
     (->> (fc/get-range2 db
                         (key-selector/first-greater-or-equal (fsub/pack subspace prefix-k))
                         (key-selector/first-greater-than (.-end (fsub/range subspace)))
                         {:limit limit
                          #_#_:keyfn #(update % 1 ->value)})
          (map (comp second first))))))



(comment
  (set-k db "store" (->buffer "foo"))
  (->value (get-k db "store" (->buffer "foo")))
  (get-k db "store" (->buffer "dafdsa"))
  (delete-k db "store" (->buffer "foo"))

  (->> (for [i (range 10)]
         (str "foo" i))
       (map ->buffer)
       (set-ks db "store"))

  (->> (get-range db "store" (->buffer "foo") (->buffer "foo2"))
       (map ->value))

  (->> (get-range db "store" (->buffer "foo") (->buffer "foo5") 3)
       (map ->value))

  (->> (seek db "store" (->buffer "foo"))
       (map #(try (->value %) (catch Exception e :some-error))))

  (->> (seek db "store" (->buffer "foo") 5)
       (map #(try (->value %) (catch Exception e :some-error))))
  )
