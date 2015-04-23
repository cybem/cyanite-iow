(ns org.spootnik.cyanite.store
  "Implements a metric store on top of cassandra. This currently
   relies on a single schema. All cassandra interaction bits
   should quickly be abstracted with a protocol to more easily
   swap implementations"
  (:require [clojure.string              :as str]
            [qbits.alia                  :as alia]
            [qbits.alia.policy.load-balancing :as alia_lbp]
            [org.spootnik.cyanite.util   :refer [partition-or-time
                                                 go-while
                                                 go-catch
                                                 counter-inc!
                                                 agg-fn-by-path
                                                 align-time
                                                 now
                                                 too-many-paths-ex]]
            [clojure.tools.logging       :refer [error info debug]]
            [lamina.core                 :refer [channel receive-all]]
            [clojure.core.async :as async :refer [<! >! go chan close!]]
            [cheshire.core :as json]
            [qbits.knit :as knit])
  (:import [com.datastax.driver.core
            BatchStatement
            PreparedStatement
            Session]
           [java.util.concurrent ExecutorService]))

(set! *warn-on-reflection* true)

(defprotocol Metricstore
  (insert [this ttl data tenant rollup period path time])
  (channel-for [this])
  (fetch [this agg paths tenant rollup period from to])
  (shutdown [this]))

;;
;; The following contains necessary cassandra queries. Since
;; cyanite relies on very few queries, I decided against using
;; hayt

(defn insertq
  "Yields a cassandra prepared statement of 7 arguments:

* `ttl`: how long to keep the point around
* `metric`: the data point
* `tenant`: tenant identifier
* `rollup`: interval between points at this resolution
* `period`: rollup multiplier which determines the time to keep points for
* `path`: name of the metric
* `time`: timestamp of the metric, should be divisible by rollup"
  [session]
  (alia/prepare
   session
   (str
    "UPDATE metric USING TTL ? SET data = data + ? "
    "WHERE tenant = ? AND rollup = ? AND period = ? AND path = ? AND time = ?;")))

(defn- batch
  "Creates a batch of prepared statements"
  [^PreparedStatement s values]
  (let [b (BatchStatement.)]
    (doseq [v values]
      (.add b (.bind s (into-array Object v))))
    b))

(defn deref-limiter
  "Deref with timeout limiter"
  [f]
  (let [result (deref f 300000 :timeout)]
    (when (= result :timeout)
      (throw (ex-info "Too long!" {})))
    result))

(defmacro time-to-ndx
  [from rollup time]
  `(/ (- ~time ~from) ~rollup))

(defn points-to-json
  [points]
  (json/generate-string (vec points)))

(defn process-path-fn
  [^Session session cql tenant rollup period from to asize]
  (let [rollup (int rollup)
        from (long from)
        to (long to)]
    (fn [path]
      (try
        (let [points (object-array asize)
              agg-fn (agg-fn-by-path path)
              rows (.execute session (format cql path))
              first-row (.one rows)]
          (when first-row
            (loop [row first-row]
              (when row
                (let [time (long (.getLong row "time"))
                      metric-raw (into [] (.getList row "data" java.lang.Double))
                      metric (if (> (count metric-raw) 1)
                               (agg-fn metric-raw)
                               (first metric-raw))]
                  (aset points (time-to-ndx from rollup time) metric)
                  (recur (.one rows)))))
            (str/join ["\"" path "\":" (points-to-json points)])))
        (catch  Exception e
          (info e "Fetching exception"))))))

(defn par-fetch
  "Fetch data in parallel fashion."
  [session executor paths tenant rollup period from to]
  (let [asize (inc (time-to-ndx from rollup to))
        cql (format (str "SELECT data, time FROM metric WHERE "
                         "path = '%%s' AND tenant = '%s' AND rollup = %s "
                         "AND period = %s AND time >= %s AND time <= %s;")
                    tenant rollup period from to)
        process-path (process-path-fn session cql tenant rollup period from
                                      to asize)
        futures (doall (map #(knit/future executor (process-path %)) paths))]
    (str "{" (str/join "," (remove nil? (map deref-limiter futures))) "}")))

(defn store-payload
  [payload session insert!]
  (try
    (let [values (map
                  #(let [{:keys [metric tenant path time rollup period ttl]} %]
                     (counter-inc! (keyword (str "tenants." tenant ".write_count")) 1)
                     [(int ttl) [metric] tenant (int rollup) (int period) path time])
                  payload)]
      (alia/execute-async
       session
       (batch insert! values)
       {:consistency :any
        :success (fn [_]
                   (debug "written batch:" (count values))
                   (counter-inc! :store.success (count values)))
        :error (fn [e]
                 (info "Casandra error: " e)
                 (counter-inc! :store.error (count values)))}))
    (catch Exception e
      (info e "Store processing exception"))))

(defn cassandra-metric-store
  "Connect to cassandra and start a path fetching thread.
   The interval is fixed for now, at 1minute"
  [{:keys [keyspace cluster hints chan_size batch_size query_paths_threshold]
    :or   {hints {:replication {:class "SimpleStrategy"
                                :replication_factor 1}}
           chan_size 10000
           batch_size 100
           query_paths_threshold nil}}]
  (info "creating cassandra metric store")
  (let [cluster (if (sequential? cluster) cluster [cluster])
        session (-> (alia/cluster
                     {:contact-points cluster
                      :compression :lz4
                      :pooling-options {:max-connections-per-host
                                        {:local 8192
                                         :remote 8192}
                                        :max-simultaneous-requests-per-connection
                                        {:local 128
                                         :remote 128}
                                        :load-balancing-policy
                                        (alia_lbp/token-aware-policy
                                         (alia_lbp/dc-aware-round-robin-policy
                                          "us-east"))}
                      :socket-options {:read-timeout-millis 1000000
                                       :receive-buffer-size 8388608
                                       :send-buffer-size 1048576
                                       :tcp-no-delay? false}})
                    (alia/connect keyspace))
        insert! (insertq session)
        executor (knit/executor :cached)
        ch (chan chan_size)
        data-stored? (atom false)]
    (reify
      Metricstore
      (channel-for [this]
        (let [ch-p (partition-or-time batch_size ch batch_size 5)]
          (go-while (not @data-stored?)
                    (let [payload (<! ch-p)]
                      (if payload
                        (store-payload payload session insert!)
                        (when (not @data-stored?)
                          (info "All data has been stored")
                          (swap! data-stored? (fn [_] true))))))
          ch))
      (insert [this ttl data tenant rollup period path time]
        (alia/execute-async
         session
         insert!
         {:values [ttl data tenant rollup period path time]}))
      (fetch [this agg paths tenant rollup period from to]
        (debug "fetching paths from store: " paths tenant rollup period from to)
        (let [paths-count (count paths)]
          (when (and query_paths_threshold (> paths-count query_paths_threshold))
            (throw (too-many-paths-ex :metric-fetch paths-count))))
        (let [min-point  (align-time from rollup)
              max-point  (align-time (apply min [to (now)]) rollup)]
          (if-let [series (and (seq paths)
                               (par-fetch session executor paths tenant rollup
                                          period min-point max-point))]
            (format "{\"from\":%s,\"to\":%s,\"step\":%s,\"series\":%s}"
                    min-point max-point rollup series)
            (json/generate-string {:from from
                                   :to to
                                   :step rollup
                                   :series {}}))))
      (shutdown [this]
        (info "Shutting down the store...")
        (close! ch)
        (while (not @data-stored?)
          (Thread/sleep 100))
        (.close session)
        (info "The store has been down")))))
