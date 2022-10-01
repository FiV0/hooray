(ns hooray.constants
  (:import (java.util.concurrent TimeUnit)))

(defonce year (* (.. TimeUnit/DAYS (toMillis 1)) 365))
(defonce day (.. TimeUnit/DAYS (toMillis 1)))
(defonce hour (.. TimeUnit/HOURS (toMillis 1)))
(defonce minute (.. TimeUnit/MINUTES (toMillis 1)))
(defonce sec (.. TimeUnit/SECONDS (toMillis 1)))
