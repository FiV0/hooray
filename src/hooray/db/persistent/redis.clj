(ns hooray.db.persistent.redis
  (:require [taoensso.carmine :as car :refer [wcar]]))


(defonce my-conn-pool   (car/connection-pool {})) ; Create a new stateful pool
(def     my-conn-spec-1 {:uri "redis://localhost:6379/"})

(def my-wcar-opts
  {:pool my-conn-pool
   :spec my-conn-spec-1})


(wcar my-wcar-opts (car/ping))

(wcar my-wcar-opts (car/zadd :store 0 "foo"))
(wcar my-wcar-opts (car/zadd :store 0 "foo1"))
(wcar my-wcar-opts (car/zadd :store 0 "foo2"))
(wcar my-wcar-opts (car/zadd :store 12 "foo3"))


(wcar my-wcar-opts (car/zrange :store 0 10))
(wcar my-wcar-opts (car/zrangebylex :store "foo" "foo2"))
(wcar my-wcar-opts (car/zrangebylex :store "foo" "foo2"))
