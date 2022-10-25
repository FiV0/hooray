(ns user
  (:require [clojure.java.io :as io]
            [clojure.pprint]
            [clojure.spec.alpha :as s]
            [clojure.spec.test.alpha :as st]
            [clojure.tools.logging :as log]
            [clojure.tools.namespace.repl :as repl]
            [lambdaisland.classpath.watch-deps :as watch-deps]
            [unilog.config :as ul]))

(defn watch-deps!
  []
  (watch-deps/start! {:aliases [:dev :test]}))

(defn go []
  (watch-deps!))


(defn better-logging! []
  (ul/start-logging! {:external  false
                      :console   "%d{yyyy-MM-dd HH:mm:ss.SSS} %highlight(%-5level) %logger{36} - %msg%n"
                      :level     :info
                      :overrides {"hooray"                                     :debug}}))

(better-logging!)
(s/check-asserts true)
(st/instrument)

(comment
  (repl/set-refresh-dirs (io/file "src") (io/file "dev"))
  (repl/refresh)
  (repl/clear)

  (watch-deps!))

;; solving cider's default printing problem for records
(defn pprint-record [r]
  (print-method r *out*))

(defn- use-method
  "Installs a function as a new method of multimethod associated with dispatch-value. "
  [^clojure.lang.MultiFn multifn dispatch-val func]
  (. multifn addMethod dispatch-val func))

(use-method clojure.pprint/simple-dispatch clojure.lang.IRecord pprint-record)
(prefer-method clojure.pprint/simple-dispatch clojure.lang.IRecord clojure.lang.IPersistentMap)
