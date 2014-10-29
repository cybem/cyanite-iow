(ns org.spootnik.cyanite.store_mware
  "Caching facility for Cyanite"
  (:require [clojure.string :as str]
            [org.spootnik.cyanite.store :as store]))

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
    (channel-for [this])
    (insert [this ttl data tenant rollup period path time]
      (put! cache tenant period rollup time path data ttl))
    (fetch [this agg paths tenant rollup period from to]
      (store/fetch store agg paths tenant rollup period from to))))
