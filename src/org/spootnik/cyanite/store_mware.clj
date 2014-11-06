(ns org.spootnik.cyanite.store_mware
  "Caching facility for Cyanite"
  (:require [clojure.string :as str]
            [clojure.core.async :as async :refer [<! >! go chan]]
            [clojure.tools.logging :refer [error info debug]]
            [org.spootnik.cyanite.store :as store]
            [org.spootnik.cyanite.store_cache :as cache]
            [org.spootnik.cyanite.util :refer [go-forever]]))

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
  [{:keys [store store-cache chan_size] :or {chan_size 10000}}]
  (info "creating caching metric store middleware")
  (let [schan (store/channel-for store)
        fn-store (partial cache/store-chan schan)]
    (reify
      store/Metricstore
      (channel-for [this]
        (let [ch (chan chan_size)]
          (go-forever
           (let [data (<! ch)
                 {:keys [metric tenant path time rollup period ttl]} data]
             (cache/put! store-cache tenant (int period) (int rollup) time path
                         metric (int ttl) cache/agg-avg fn-store)))
          ch))
      (insert [this ttl data tenant rollup period path time]
        (cache/put! store-cache tenant period rollup time path data ttl
                    cache/agg-avg fn-store))
      (fetch [this agg paths tenant rollup period from to]
        (store/fetch store agg paths tenant rollup period from to)))))
