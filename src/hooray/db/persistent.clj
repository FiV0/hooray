(ns hooray.db.persistent
  (:require [hooray.db :as db]))

(defmethod db/connect* :persistent [uri-map]
  (throw (ex-info "Persistent connections are currently not supported!" uri-map)))
