(ns org.spootnik.cyanite.store
  "Implements a metric store on top of cassandra. This currently
   relies on a single schema. All cassandra interaction bits
   should quickly be abstracted with a protocol to more easily
   swap implementations"
  (:require [clojure.string              :as str]
            [qbits.alia                  :as alia]
            [org.spootnik.cyanite.util   :refer [partition-or-time
                                                 go-forever go-catch
                                                 counter-inc!
                                                 agg-fn-by-path
                                                 align-time
                                                 now]]
            [clojure.tools.logging       :refer [error info debug]]
            [lamina.core                 :refer [channel receive-all]]
            [clojure.core.async :as async :refer [<! >! go chan]]
            [cheshire.core :as json])
  (:import [com.datastax.driver.core
            BatchStatement
            PreparedStatement]))

(set! *warn-on-reflection* true)

(defprotocol Metricstore
  (insert [this ttl data tenant rollup period path time])
  (channel-for [this])
  (fetch [this agg paths tenant rollup period from to]))

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

(defn fetchq
  "Yields a cassandra prepared statement of 7 arguments:

* `paths`: list of paths
* `tenant`: tenant identifier
* `rollup`: interval between points at this resolution
* `period`: rollup multiplier which determines the time to keep points for
* `min`: return points starting from this timestamp
* `max`: return points up to this timestamp
* `limit`: maximum number of points to return"
  [session]
  (alia/prepare
   session
   (str
    "SELECT data,time FROM metric WHERE "
    "path = ? AND tenant = ? AND rollup = ? AND period = ? "
    "AND time >= ? AND time <= ?;")))


(defn useq
  "Yields a cassandra use statement for a keyspace"
  [keyspace]
  (format "USE %s;" (name keyspace)))

(defn max-points
  "Returns the maximum number of points to expect for
   a given resolution, time range and number of paths"
  [paths rollup from to]
  (-> (- to from)
      (/ rollup)
      (long)
      (inc)
      (* (count paths))
      (int)))

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

(defn par-fetch
  "Fetch data in parallel fashion."
  [session fetch! paths tenant rollup period from to]
  (let [rollup (int rollup)
        from (long from)
        to (long to)
        asize (inc (time-to-ndx from rollup to))
        series (atom ())
        futures
        (doall (map #(future
                       (try
                         (let [points (object-array asize)
                               agg-fn (agg-fn-by-path %)
                               rows (->> (alia/execute
                                          session fetch!
                                          {:values [% tenant (int rollup)
                                                    (int period)
                                                    from to]
                                           :fetch-size Integer/MAX_VALUE}))]
                           (when rows
                             (doseq [row rows]
                               (let [time (long (:time row))
                                     metric-raw (:data row)
                                     metric (if (> (count metric-raw) 1)
                                              (agg-fn metric-raw)
                                              (first metric-raw))]
                                 (aset points (time-to-ndx from rollup time)
                                       metric)))
                             (let [jpoints (str/join ["\"" % "\":"
                                                      (points-to-json points)])]
                               (swap! series (fn [s]
                                               (if (empty? s)
                                                 (list jpoints)
                                                 (conj s jpoints)))))))
                         (catch  Exception e
                           (info e "Fetching exception"))))
                    paths))]
    (doall (map deref-limiter futures))
    (str/join ["{" (str/join "," @series) "}"])))

(defn cassandra-metric-store
  "Connect to cassandra and start a path fetching thread.
   The interval is fixed for now, at 1minute"
  [{:keys [keyspace cluster hints chan_size batch_size]
    :or   {hints {:replication {:class "SimpleStrategy"
                                :replication_factor 1}}
           chan_size 10000
           batch_size 100}}]
  (info "creating cassandra metric store")
  (let [cluster (if (sequential? cluster) cluster [cluster])
        session (-> (alia/cluster {:contact-points cluster
                                   :pooling-options {:max-connections-per-host {:local 8192
                                                                                :remote 8192}}})
                    (alia/connect keyspace))
        insert! (insertq session)
        fetch!  (fetchq session)]
    (reify
      Metricstore
      (channel-for [this]
        (let [ch (chan chan_size)
              ch-p (partition-or-time batch_size ch batch_size 5)]
          (go-forever
           (let [payload (<! ch-p)]
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
                 (info e "Store processing exception")))))
          ch))
      (insert [this ttl data tenant rollup period path time]
        (alia/execute-async
         session
         insert!
         {:values [ttl data tenant rollup period path time]}))
      (fetch [this agg paths tenant rollup period from to]
        (debug "fetching paths from store: " paths tenant rollup period from to)
        (let [min-point  (align-time from rollup)
              max-point  (align-time (apply min [to (now)]) rollup)]
          (if-let [series (and (seq paths)
                               (par-fetch session fetch! paths tenant rollup
                                          period min-point max-point))]
            (format "{\"from\":%s,\"to\":%s,\"step\":%s,\"series\":%s}"
                    min-point max-point rollup series)
            (json/generate-string {:from from
                                   :to to
                                   :step rollup
                                   :series {}})))))))
