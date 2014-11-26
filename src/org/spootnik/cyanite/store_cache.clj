(ns org.spootnik.cyanite.store_cache
  "Caching facility for Cyanite"
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [error info debug]]
            [org.spootnik.cyanite.store :as store]
            [org.spootnik.cyanite.util :refer [agg-fn-by-path]]))

(defprotocol StoreCache
  (put! [this tenant period rollup time path data ttl fn-agg fn-store])
  (flush! [this])
  (-show-keys [this])
  (-show-cache [this])
  (-show-meta [this]))

(def ^:const metric-wait-time 60)
(def ^:const cache-add-ttl 180)

(defn- construct-mkey
  [tenant period rollup time]
  (str/join "-" [tenant period rollup time]))

(defn- construct-key
  ([tenant period rollup time path]
     (str/join "-" [(construct-mkey tenant period rollup time) path]))
  ([mkey path]
     (str/join "-" [mkey path])))

(defn- calc-delay
  [rollup add]
  (+ rollup metric-wait-time add))

(defn- to-ms
  [sec]
  (* sec 1000))

(defn- create-flusher
  [mkeys mkey pkeys tenant period rollup time ttl fn-get fn-agg fn-store fn-key]
  (fn []
    (try
      (doseq [path @pkeys]
        (let [fn-agg (or fn-agg (agg-fn-by-path path))]
          (fn-store tenant period rollup time path
                    (fn-agg (fn-get
                             (fn-key tenant period rollup time path)
                             @pkeys)) ttl)))
      (catch Exception e
        (error e "Cache flushing exception"))
      (finally
        (swap! mkeys #(dissoc % mkey))))))

(defn- run-delayer!
  [rollup fn-flusher]
  (let [delayer (future
                  (Thread/sleep
                   (to-ms (calc-delay rollup (rand-int (dec (int rollup)))))))
        flusher (future
                  (try
                    (deref delayer)
                    (catch Exception _))
                  (fn-flusher))]
    [delayer flusher]))

(defn- set-pkeys!
  [mkeys tenant period rollup time ttl fn-get fn-agg fn-store fn-key]
  (let [mkey (construct-mkey tenant period rollup time)]
    (swap! mkeys
           #(if (contains? % mkey)
              %
              (let [pkeys (atom #{})
                    fn-flusher (create-flusher mkeys mkey pkeys tenant period
                                               rollup time ttl fn-get fn-agg
                                               fn-store fn-key)
                    [delayer flusher] (run-delayer! rollup fn-flusher)]
                (swap! pkeys
                       (fn [pkeys]
                         (with-meta pkeys {:fn-flusher fn-flusher
                                           :delayer delayer
                                           :flusher flusher
                                           :data (atom {})
                                           :rollup rollup})))
                (assoc % mkey pkeys))))
    (get @mkeys mkey)))

(defn- set-keys!
  [mkeys tenant period rollup time path ttl fn-get fn-agg fn-store fn-key]
  (let [pkeys (set-pkeys! mkeys tenant period rollup time ttl fn-get
                          fn-agg fn-store fn-key)]
    (swap! pkeys #(if (contains? % path) % (conj % path)))
    pkeys))

(defn- run-flusher!
  [mkeys]
  (info "Flushing the store cache...")
  (try
    (let [sorted-mkeys (sort-by #(:rollup (meta (get mkeys (first %)))) @mkeys)]
      (doseq [[mkey pkeys] sorted-mkeys]
        (let [m (meta @pkeys)
              delayer (get m :delayer nil)
              flusher (get m :flusher nil)]
          (when delayer
            (future-cancel delayer)
            (when flusher
              (deref flusher))))))
    (info "The store cache has been flushed")
    (catch Exception e
      (error e "Cache flushing exception"))))

(defn in-memory-cache
  [config]
  (info "creating in-memory store aggregation cache")
  (let [mkeys (atom {})
        get-data (fn [pkeys] (get (meta pkeys) :data))
        cache-get (fn [key pkeys] (get @(get-data pkeys) key))
        cache-key (fn [tenant period rollup time path] (hash path))]
    (reify
      StoreCache
      (put! [this tenant period rollup time path data ttl fn-agg fn-store]
        (let [ckey (cache-key tenant period rollup time path)
              pkeys (set-keys! mkeys tenant period rollup time path ttl
                               cache-get fn-agg fn-store cache-key)
              adata (get-data @pkeys)]
          (swap! adata #(assoc % ckey (conj (get % ckey) data)))))
      (flush! [this]
        (run-flusher! mkeys))
      (-show-keys [this] (debug "MKeys:" mkeys))
      (-show-cache [this]
        (debug "Cache:")
        (doseq [[mkey pkeys] @mkeys]
          (debug (get-data @pkeys))))
      (-show-meta [this]
        (doseq [[mkey pkeys] @mkeys]
          (debug "Meta:" mkey (meta @pkeys)))))))
