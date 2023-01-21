(ns hooray.db.persistent.fdb
  (:require [hooray.db.persistent.packing :as pack]
            [hooray.db.persistent.protocols :as per]
            [me.vedang.clj-fdb.FDB :as cfdb]
            [me.vedang.clj-fdb.core :as fc]
            [me.vedang.clj-fdb.key-selector :as key-selector]
            [me.vedang.clj-fdb.range :as frange]
            [me.vedang.clj-fdb.subspace.subspace :as fsub]
            [me.vedang.clj-fdb.transaction :as ftr]
            [taoensso.nippy :as nippy])
  (:import (com.apple.foundationdb FDBDatabase KeySelector)))

;; TODO/TO consider maybe use the tuple model directly for the indices

;; A fdb object that assures the correct API version is selected.
;; DB instrances are created from this
(def fdb (cfdb/select-api-version cfdb/clj-fdb-api-version))

(comment
  (def db (cfdb/open fdb)))

(def keyspace->subspace (memoize
                         (fn [keyspace]
                           (fsub/create [keyspace]))))

(defn ->buffer [v] (nippy/freeze v))
(defn ->value [b] (nippy/thaw b))

(defn set-k [db keyspace k]
  (fc/set db (keyspace->subspace keyspace) k nil))

(defn set-ks [db keyspace ks]
  (let [subspace (keyspace->subspace keyspace)]
    (ftr/run db
      (fn [tr]
        (doseq [k ks]
          (fc/set tr subspace k nil))))))

(defn delete-k [db keyspace k]
  (fc/clear db (keyspace->subspace keyspace) k))

(defn delete-ks [db keyspace ks]
  (let [subspace (keyspace->subspace keyspace)]
    (ftr/run db
      (fn [tr]
        (doseq [k ks]
          (fc/clear tr subspace k))))))

(defn- third [c] (nth c 2))
(def ^:private seperate (juxt filter remove))

(defn upsert-ks [db ks]
  (let [key-fn #(map second %)
        [asserts deletes] (->> (seperate third ks)
                               (map (comp #(update-vals % key-fn)
                                          #(update-keys % keyspace->subspace)
                                          #(group-by first %))))]
    (ftr/run db
      (fn [tr]
        (run! (fn [[subspace ks]]
                (doseq [k ks]
                  (fc/set tr subspace k nil)))
              asserts)
        (run! (fn [[subspace ks]]
                (doseq [k ks]
                  (fc/clear tr subspace k)))
              deletes)))))

(defn get-k [db keyspace k]
  (when (fc/get db (keyspace->subspace keyspace) k) k))

;; TODO figure out how to get rid of the subspace in the keys
;; TODO figure out why first-greater-than doesn't work
(defn get-range
  ([db keyspace]
   (->> (fc/get-range db (keyspace->subspace keyspace) {:coll []})
        (map ffirst)))
  ([db keyspace prefix-k]
   (let [subspace (keyspace->subspace keyspace)]
     (->> (fc/get-range2 db
                         (key-selector/first-greater-or-equal (fsub/pack subspace prefix-k))
                         (key-selector/first-greater-or-equal (fsub/pack subspace (pack/inc-ba (pack/copy prefix-k))))
                         #_(key-selector/first-greater-than (fsub/pack subspace prefix-k))
                         #_{:keyfn #(update % 1 ->value)})
          (map (comp second first)))))
  ([db keyspace start-k stop-k]
   (let [subspace (keyspace->subspace keyspace)]
     (->> (fc/get-range2 db
                         (key-selector/first-greater-or-equal (fsub/pack subspace start-k))
                         (key-selector/first-greater-or-equal (fsub/pack subspace stop-k))
                         #_{:keyfn #(update % 1 ->value)})
          (map (comp second first)))))
  ([db keyspace start-k stop-k limit]
   (let [subspace (keyspace->subspace keyspace)]
     (->> (fc/get-range2 db
                         (key-selector/first-greater-or-equal (fsub/pack subspace start-k))
                         (key-selector/first-greater-or-equal (fsub/pack subspace stop-k))
                         {:limit limit
                          #_#_:keyfn #(update % 1 ->value)})
          (map (comp second first))))))

(defn seek
  {:arglists '([conn keyspace prefix-k] [conn keyspace prefix-k limit]
               [conn keyspace prefix-k k] [conn keyspace prefix-k k limit])}
  ([db keyspace prefix-k]
   (let [subspace (keyspace->subspace keyspace)]
     (if (empty? prefix-k)
       (get-range db keyspace)
       (->> (fc/get-range2 db
                           (key-selector/first-greater-or-equal (fsub/pack subspace prefix-k))
                           (key-selector/first-greater-or-equal (fsub/pack subspace (pack/inc-ba (pack/copy prefix-k))))
                           #_(key-selector/first-greater-than full-k)
                           #_{:keyfn #(update % 1 ->value)})
            (map (comp second first))))))
  ([db keyspace prefix-k k-or-limit]
   (if (int? k-or-limit)
     (if (empty? prefix-k)
       (->> (fc/get-range db (keyspace->subspace keyspace) {:limit k-or-limit :coll []})
            (map ffirst))
       (let [subspace (keyspace->subspace keyspace)
             #_#_full-k (fsub/pack subspace prefix-k)]
         (->> (fc/get-range2 db
                             (key-selector/first-greater-or-equal (fsub/pack subspace prefix-k))
                             (key-selector/first-greater-or-equal (fsub/pack subspace (pack/inc-ba (pack/copy prefix-k))))
                             #_(key-selector/first-greater-than full-k)
                             {:limit k-or-limit
                              #_#_:keyfn #(update % 1 ->value)})
              (map (comp second first)))))
     (seek db keyspace prefix-k k-or-limit Integer/MAX_VALUE)))
  ([db keyspace prefix-k k limit]
   (let [subspace (keyspace->subspace keyspace)
         new-k (if (empty? prefix-k)
                 k
                 (pack/concat-ba prefix-k k))
         #_#_full-k (if (empty? prefix-k)
                      (fsub/pack subspace k)
                      (fsub/pack subspace (pack/concat-ba prefix-k k)))]
     (->> (fc/get-range2 db
                         (key-selector/first-greater-or-equal (fsub/pack subspace new-k))
                         (key-selector/first-greater-or-equal (fsub/pack subspace (pack/inc-ba (pack/copy prefix-k))))
                         #_(key-selector/first-greater-than full-k)
                         {:limit limit
                          #_#_:keyfn #(update % 1 ->value)})
          (map (comp second first)))) ))

;; TODO add keycount manually
;; maybe adapt set-k/set-ks
(defn count-ks
  ([db keyspace] (throw (ex-info "key count currently not supported by fdb!" {})))
  ([db keyspace prefix-k] (throw (ex-info "key count currently not supported by fdb!" {})) )
  ([db keyspace start-k stop-k] (throw (ex-info "key count currently not supported by fdb!" {}))))

(comment
  (set-k db "store" (->buffer "foo"))
  (->value (get-k db "store" (->buffer "foo")))
  (get-k db "store" (->buffer "dafdsa"))
  (delete-k db "store" (->buffer "foo"))

  (upsert-ks db [["store" (->buffer "foo") nil] ["store" (->buffer "foo1") true] ["store1" (->buffer "foo") true]])
  (get-k db "store" (->buffer "foo"))
  (get-k db "store1" (->buffer "foo"))
  (get-k db "store" (->buffer "foo1"))

  (->> (for [i (range 200000 (- 200000 10) -1)] i)
       (map pack/int->bytes)
       (set-ks db "store"))

  (import '(java.util Arrays))

  (def try-bytes->int #(try (pack/bytes->int %) (catch Exception e :some-error)))
  (def prefix-k (Arrays/copyOfRange (pack/int->bytes 200000) 0 3))
  (def small-k (Arrays/copyOfRange (pack/int->bytes 199993) 3 4))

  (->> (get-range db "store")
       (map try-bytes->int))

  (->> (get-range db "store" prefix-k)
       (map try-bytes->int))

  (->> (get-range db "store" (pack/int->bytes (- 200000 10)) (pack/int->bytes 200000))
       (map pack/bytes->int))

  (->> (get-range db "store" (pack/int->bytes (- 200000 10)) (pack/int->bytes 200000) 3)
       (map pack/bytes->int))

  (->> (seek db "store" prefix-k)
       (map try-bytes->int))

  (->> (seek db "store" (byte-array 0))
       (map try-bytes->int))

  (->> (seek db "store" (byte-array 0) 3)
       (map try-bytes->int))

  (->> (seek db "store" prefix-k small-k)
       (map try-bytes->int))

  (->> (seek db "store" prefix-k small-k 3)
       (map try-bytes->int)))

(defrecord FDBKeyStore [conn]
  per/KeyStore
  (set-k [this keyspace k] (set-k conn (name keyspace) k))
  (set-ks [this keyspace ks] (set-ks conn (name keyspace) ks))
  (delete-k [this keyspace k] (delete-k conn (name keyspace) k))
  (delete-ks [this keyspace ks] (delete-ks conn (name keyspace) ks))
  (upsert-ks [this ops] (upsert-ks conn ops))
  (get-k [this keyspace k] (get-k conn (name keyspace) k))
  (get-range [this keyspace] (get-range conn (name keyspace)))
  (get-range [this keyspace prefix-k] (get-range conn (name keyspace) prefix-k))
  (get-range [this keyspace begin end] (get-range conn (name keyspace) begin end))
  (get-range [this keyspace begin end limit] (get-range conn (name keyspace) begin end limit))
  (seek [this keyspace prefix-k] (seek conn (name keyspace) prefix-k))
  (seek [this keyspace prefix-k limit] (seek conn (name keyspace) prefix-k limit))
  (seek [this keyspace prefix-k k limit] (seek conn (name keyspace) prefix-k k limit))
  (count-ks [this keyspace] (count-ks conn (name keyspace)))
  (count-ks [this keyspace prefix-k] (count-ks conn (name keyspace) prefix-k))
  (count-ks [this keyspace begin end] (count-ks conn (name keyspace) begin end)))

(defn ->fdb-key-store [_conn]
  (->FDBKeyStore (cfdb/open fdb)))

;; DOC STORE

(defn set-kv [db keyspace k v]
  (let [subspace (fsub/create [keyspace])]
    (fc/set db subspace k v)))

(defn set-kvs [db keyspace kvs]
  (let [subspace (fsub/create [keyspace])]
    (ftr/run db
      (fn [tr]
        (doseq [[k v] kvs]
          (fc/set tr subspace k v))))))

(defn get-kv [db keyspace k]
  (let [subspace (fsub/create [keyspace])]
    (fc/get db subspace k)))

(defn get-kvs [db keyspace ks]
  (let [subspace (fsub/create [keyspace])]
    (ftr/run db
      (fn [tr]
        (doall (map (partial fc/get tr subspace) ks))))))

(defn delete-kv [db keyspace k]
  (let [subspace (fsub/create [keyspace])]
    (fc/clear db subspace k)))

(defn delete-kvs [db keyspace ks]
  (let [subspace (fsub/create [keyspace])]
    (ftr/run db
      (fn [tr]
        (run! (partial fc/clear tr subspace) ks)))))

(defn upsert-kvs [db kvs]
  (let [key-fn #(map second %)
        [asserts deletes] (->> (seperate third kvs)
                               (map (comp #(update-vals % key-fn)
                                          #(update-keys % keyspace->subspace)
                                          #(group-by first %))))]
    (ftr/run db
      (fn [tr]
        (run! (fn [[subspace kvs]]
                (doseq [[k v] kvs]
                  (fc/set tr subspace k v)))
              asserts)
        (run! (fn [[subspace ks]]
                (doseq [k ks]
                  (fc/clear tr subspace k)))
              deletes)))))


(comment
  (set-kv db "doc-store" (->buffer "foo") (->buffer "bar"))
  (->value (get-kv db "doc-store" (->buffer "foo")))
  (set-kvs db "doc-store" [[(->buffer "foo") (->buffer "bar")] [(->buffer "foo0") (->buffer "bar0")]])
  (->> (get-kvs db "doc-store" [(->buffer "foo") (->buffer "foo0")])
       (map ->value))
  (delete-kv db "doc-store" (->buffer "foo0"))
  (get-kv db "doc-store" (->buffer "foo0"))
  (delete-kvs db "doc-store" [(->buffer "foo") (->buffer "foo0")])
  (get-kv db "doc-store" (->buffer "foo"))
  (upsert-kvs db [["doc-store" (->buffer "foo") nil]
                  ["doc-store" [(->buffer "foo1") (->buffer "bar")] true]
                  ["doc-store1" [(->buffer "foo") (->buffer "bar")] true]])
  (get-kv db "doc-store" (->buffer "foo"))
  (->value (get-kv db "doc-store" (->buffer "foo1")))
  (->value (get-kv db "doc-store1" (->buffer "foo"))))

;; ADMIN

(defn clear-set
  "WARNING! This clears the entire keyspace."
  [db keyspace]
  (let [subspace (fsub/create [keyspace])]
    (fc/clear-range db (fsub/range subspace))))

(def ^:private smallest-ba (byte-array [(unchecked-byte 0x01)]))
(def ^:private largest-ba (byte-array [(unchecked-byte 0xff)]))

(defn clear-db
  "WARNING! This clears the entire db."
  [db]
  (fc/clear-range db (frange/range smallest-ba largest-ba)))

(comment
  (clear-set db "store")
  (clear-db db))

;;///////////////////////////////////////////////////////////////////////////////
;;===============================================================================
;;                                  Connection
;;===============================================================================
;;\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\

(defmethod per/config-map->conn :fdb [config-map]
  (if-let [cluster-file (-> config-map :spec :cluster-file)]
    (cfdb/open fdb cluster-file)
    (cfdb/open fdb)))

(defn fdb-connection? [conn]
  (instance? conn FDBDatabase))

(defn close-connection [conn]
  {:pre [(fdb-connection? conn)]}
  (.close conn))
