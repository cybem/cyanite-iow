(ns org.spootnik.cyanite.store_mware
  "Caching facility for Cyanite"
  (:require [clojure.string :as str]
            [clojure.core.async :as async :refer [<! >! >!! go chan close!]]
            [clojure.tools.logging :refer [error info debug]]
            [org.spootnik.cyanite.store :as store]
            [org.spootnik.cyanite.store_cache :as cache]
            [org.spootnik.cyanite.util :refer [go-while aggregate-with]]))

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
      (store/fetch store agg paths tenant rollup period from to))
    (shutdown [this])))

(defn- store-chan
  [chan tenant period rollup time path data ttl]
  (>!! chan {:tenant tenant
             :period period
             :rollup rollup
             :time   time
             :path   path
             :metric data
             :ttl    ttl}))

(defn- store-agg-fn
  [store-cache schan rollups min-rollup]
  (let [fn-store (partial store-chan schan)
        rollups (filter #(not= (:rollup %) min-rollup) rollups)
        agg-avg (partial aggregate-with :avg)]
    (fn [tenant period rollup time path data ttl]
      (fn-store tenant period rollup time path data ttl)
      (doseq [{:keys [rollup period ttl rollup-to]} rollups]
        (cache/put! store-cache tenant period rollup (rollup-to time) path data
                    ttl agg-avg fn-store)))))

(defn- put!
  [min-rollup store-cache fn-store tenant period rollup time path metric ttl]
  (when (= min-rollup rollup)
    (cache/put! store-cache tenant (int period) (int rollup) time
                path metric (int ttl) nil fn-store)))

(defn store-middleware-cache
  [{:keys [store store-cache chan_size rollups] :or {chan_size 10000}}]
  (info "creating caching metric store middleware")
  (let [min-rollup (:rollup (first (sort-by :rollup rollups)))
        fn-store (store-agg-fn store-cache (store/channel-for store) rollups
                               min-rollup)
        fn-put! (partial put! min-rollup store-cache fn-store)
        ch (chan chan_size)
        data-processed? (atom false)]
    (reify
      store/Metricstore
      (channel-for [this]
        (go-while (not @data-processed?)
                  (let [data (<! ch)]
                    (if data
                      (let [{:keys [metric tenant path time rollup period ttl]} data]
                        (fn-put! tenant period rollup time path metric ttl))
                      (when (not @data-processed?)
                        (info "All data has been stored in the cache")
                        (swap! data-processed? (fn [_] true))))))
        ch)
      (insert [this ttl data tenant rollup period path time]
        (fn-put! tenant period rollup time path data ttl))
      (fetch [this agg paths tenant rollup period from to]
        (store/fetch store agg paths tenant rollup period from to))
      (shutdown [this]
        (info "Shutting down the store middleware...")
        (close! ch)
        (while (not @data-processed?)
          (Thread/sleep 100))
        (cache/flush! store-cache)
        (info "The store middleware has been down")))))
