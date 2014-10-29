(ns org.spootnik.cyanite.store_mware
  "Caching facility for Cyanite"
  (:require [clojure.string :as str]
            [org.spootnik.cyanite.store :as store]
            [clojure.tools.logging :refer [error info debug]]))

(defn store-middleware
  [{:keys [store]}]
  (info "creating defalut metric store middleware")
  (reify
    store/Metricstore
    (channel-for [this]
      (store/channel-for store))
    (insert [this ttl data tenant rollup period path time]
      (store/insert this ttl data tenant rollup period path time))
    (fetch [this agg paths tenant rollup period from to]
      (store/fetch store agg paths tenant rollup period from to))))

(defn store-middleware-cache
  [{:keys [store cache]}]
  (info "creating caching metric store middleware")
  (reify
    store/Metricstore
    (channel-for [this]
      (let [ch (chan chan_size)]
        (go-forever
         (let [data (<! ch)
               {:keys [metric tenant path time rollup period ttl]} data]
           (cache/put! cache tenant (int period) (int rollup) time path
                       metric (int ttl))))
        ch))
    (insert [this ttl data tenant rollup period path time]
      (cache/put! cache tenant period rollup time path data ttl))
    (fetch [this agg paths tenant rollup period from to]
      (store/fetch store agg paths tenant rollup period from to))))
