(ns org.spootnik.cyanite.carbon
  "Dead simple carbon protocol handler"
  (:require [aleph.tcp                  :as tcp]
            [clojure.string             :as s]
            [org.spootnik.cyanite.store :as store]
            [org.spootnik.cyanite.path  :as path]
            [org.spootnik.cyanite.tcp   :as tc]
            [org.spootnik.cyanite.util  :refer [partition-or-time get-aggregator-patterns
                                                get-blacklist-patterns
                                                counter-get counter-list
                                                counters-reset! counter-inc!
                                                go-forever]]
            [clojure.tools.logging        :refer [info debug]]
            [gloss.core                   :refer [string]]
            [lamina.core                  :refer :all]
            [clojure.core.async :as async :refer [<! >! >!! go chan timeout]]))

(set! *warn-on-reflection* true)

(defn make-aggregate-paths [path tenant]
  (if-let [pattern-list (get-aggregator-patterns tenant)]
    (do
      (debug "Doing regex thing, got expressions: " pattern-list)
      (distinct (concat (into [] (map (fn [[k v]] (clojure.string/replace path k v)) pattern-list)) [path])))
    (do
      (debug "got nothing to add: " [path])
      [path])))

(defn check-blacklist [paths tenant]
  (if-let [pattern-list (get-blacklist-patterns tenant)]
    (do
      (debug "Got blacklist: " pattern-list)
      (remove (set (distinct (flatten (for [p pattern-list] (filter #(re-matches (re-pattern p) %) paths))))) paths))
    (do
      (debug "No blacklist:" paths)
      paths)))

(defn parse-num
  "parse a number into the given value, return the
  default value if it fails"
  [parse default number]
  (try (parse number)
    (catch Exception e
      (debug "got an invalid number" number (.getMessage e))
      default)))

(defn formatter
  "Split each line on whitespace, discard nan metric lines
   and format correct lines for each resolution"
  [rollups ^String input]
  (try
    (let [[path metric time tenant] (s/split (.trim input) #" ")
          paths (check-blacklist (make-aggregate-paths path tenant) tenant)
          timel (parse-num #(Long/parseLong %) "nan" time)
          metricd (parse-num #(Double/parseDouble %) "nan" metric)
          tenantstr (or tenant "NONE")]
      (counter-inc! (keyword (str "tenants." tenant ".metrics_received")) 1)
      (when (and (not= "nan" metricd) (not= "nan" timel))
        (for [path paths
              {:keys [rollup period ttl rollup-to]} rollups]
          {:path   path
           :tenant tenantstr
           :rollup rollup
           :period period
           :ttl    (or ttl (* rollup period))
           :time   (rollup-to timel)
           :metric metricd}
          )
        )
      )
    (catch Exception e
      (info "Exception for metric [" input "] : " e))))

(defn format-processor
  "Send each metric over to the cassandra store"
  [chan indexch rollups insertch]
  (go
    (let [input (partition-or-time 1000 chan 500 5)]
      (while true
        (let [metrics (<! input)]
          (try
            (counter-inc! :metrics_received (count metrics))
            (doseq [metric metrics]
              (let [formed (remove nil? (formatter rollups metric))]
                (doseq [f formed]
                  (>! insertch f))
                (doseq [p (distinct (map (juxt :path :tenant) formed))]
                  (>! indexch p))))
            (catch Exception e
              (info "Exception for metric [" metrics "] : " e))))))))

(defn start
  "Start a tcp carbon listener"
  [{:keys [store-middleware carbon index stats]}]
  (let [indexch (path/channel-for index)
        insertch (store/channel-for store-middleware)
        chan (chan 100000)
        handler (format-processor chan indexch (:rollups carbon) insertch)]
    (info "starting carbon handler: " carbon)
    (go
      (let [{:keys [hostname tenant interval console]} stats]
        (while true
          (<! (timeout (* interval 1000)))
          (doseq [[k _]  (counter-list)]
            (if console (info "Stats: " k "=" (counter-get k)))
            (>! chan (clojure.string/join " " [(str hostname ".cyanite." (name k))
                                               (counter-get k)
                                               (quot (System/currentTimeMillis) 1000)
                                               tenant])))
          (counters-reset!))))
    (tc/start-tcp-server
     (merge carbon {:response-channel chan}))))
