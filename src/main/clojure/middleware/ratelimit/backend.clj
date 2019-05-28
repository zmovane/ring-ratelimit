(ns middleware.ratelimit.backend
  (:require [taoensso.carmine :as car]))

(defprotocol Backend
  (get-bucket [self bucket-key])
  (update-bucket [self bucket-key bucket]))


(deftype RedisBackend [redis-conf hash-key] Backend

  (get-bucket [self bucket-key]
    (car/wcar redis-conf
      (car/hget hash-key bucket-key)))

  (update-bucket [self bucket-key bucket]
    (car/wcar redis-conf
      (car/hset hash-key bucket-key bucket))))


(deftype LocalAtomBackend [buckets] Backend

  (get-bucket [self bucket-key]
    (get @buckets bucket-key))

  (update-bucket [self bucket-key bucket]
    (swap! buckets assoc bucket-key bucket)))


(def ^:private default-buckets (atom {}))

(defn local-atom-backend
  ([]
   (local-atom-backend default-buckets))
  ([buckets]
   (LocalAtomBackend. buckets)))


(def ^:private default-opts {:pool {} :spec {:host "127.0.0.1" :port 6379 :db 0}})

(defn redis-backend
  ([]
   (redis-backend default-opts "ratelimits"))
  ([conn-opts]
   (redis-backend conn-opts "ratelimits"))
  ([conn-opts key]
   (RedisBackend. conn-opts key)))