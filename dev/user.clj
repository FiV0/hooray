(ns user
  (:require [clojure.java.io :as io]
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
(st/instrument)

(comment
  (repl/set-refresh-dirs (io/file "src") (io/file "dev"))
  (repl/refresh)
  (repl/clear)

  (watch-deps!))

(comment
  (require '[clojure.spec.alpha :as s])

  (s/def ::tuple (s/and vector?
                        (s/cat :first identity :second identity)))

  (def tuple (s/conform ::tuple [1 2]))

  (defn foo [t] t)

  (s/fdef foo :args (s/cat :t ::tuple))

  (foo tuple))
