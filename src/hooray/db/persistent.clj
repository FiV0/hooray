(ns hooray.db.persistent
  (:require [hooray.db :as db]))

(defmethod db/connect :persistent [uri]
  (throw (ex-info "Persistent connections are currently not supported!" {:uri uri})))
