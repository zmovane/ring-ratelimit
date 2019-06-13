(ns middleware.ratelimit.core
  (:require [middleware.ratelimit.backend :as backend]
            [clojure.string :as str])
  (:import (java.util.concurrent TimeUnit)))

(defn wrap-ratelimit [handler & {:keys [interval interval-time-unit limit select-key-fn whitelist fail-response backend path-limit?]
                                 :or   {select-key-fn #(:remote-addr %)
                                        whitelist     []
                                        fail-response "Too Many Requests"
                                        backend       (backend/local-atom-backend)
                                        path-limit?   false}}]
  (fn [req respond raise]
    (if-let [bucket-key (select-key-fn req)]
      (if-not (.contains whitelist bucket-key)
        (let [bucket-key        (let [scheme (get-in req [:headers "upgrade"] "http")
                                      path   (get req :uri "")
                                      frags  [bucket-key scheme]]
                                  (str/join ":" (if path-limit? (conj frags path) frags)))
              tokens-per-millis (/ limit (.toMillis interval-time-unit interval))
              bucket            (backend/get-bucket backend bucket-key)
              now               (System/currentTimeMillis)
              last              (:timestamp bucket)
              remaining         (-> (if (some? bucket)
                                      (* (- now last)
                                         tokens-per-millis)
                                      limit)
                                    (+ (get bucket :remaining 0))
                                    (min limit))
              over-limit?       (< remaining 1)
              retry-after       (if over-limit?
                                  (-> (- 1 remaining)
                                      (/ tokens-per-millis 1000)
                                      Math/ceil
                                      long)
                                  0)
              rl-headers        {:x-ratelimit-limit     limit
                                 :x-ratelimit-remaining (int remaining)
                                 :x-ratelimit-reset     (+ (quot now 1000) retry-after)}]
          (if over-limit?
            (let [rl-headers (assoc rl-headers :retry-after retry-after)]
              (respond {:status 429 :body fail-response :headers rl-headers}))
            (do
              (backend/update-bucket backend bucket-key {:timestamp now :remaining (dec remaining)})
              (let [headers-fn #(merge % rl-headers)
                    respond-fn #(respond (update % :headers headers-fn))]
                (handler req respond-fn raise)))))
        (handler req respond raise))
      (handler req respond raise))))