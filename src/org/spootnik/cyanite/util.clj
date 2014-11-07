(ns org.spootnik.cyanite.util
  (:require [clojure.core.async :as async :refer [alt! chan >! close! go
                                                  timeout >!! go-loop
                                                  dropping-buffer]]
            [clojure.tools.logging :refer [info warn error]]))

(defmacro go-forever
  [body]
  `(go
     (while true
       (try
         ~body
         (catch Exception e
           (error e (or (.getMessage e)
                        "Exception while processing channel message")))))))

(defn map-vals
  "Given a map and a function, returns the map resulting from applying
  the function to each value."
  [m f]
  (zipmap (keys m) (map f (vals m))))

(defn map-kv
  "Given a map and a function of two arguments, returns the map
  resulting from applying the function to each of its entries. The
  provided function must return a pair (a two-element sequence.)"
  [m f]
  (into {} (map (fn [[k v]] (f k v)) m)))

(defn parse-regex [k v] [(-> k
                             (clojure.string/replace "." "\\.")
                             (clojure.string/replace "*" ".*?")
                             (clojure.string/replace #"<([a-z]+)>" "(?<$1>.*?)")
                             (re-pattern))
                         (-> v (clojure.string/replace #"<([a-z]+)>" "\\${$1}"))])

(defn convert-regex [regex-map]
  (map-kv regex-map parse-regex))

(comment "Counters are predefined to fill graphs with 0s")

(def counters (atom { :index.create 0
                      :index.get_error 0
                      :store.success 0
                      :store.error 0
                      :metrics_received 0}))

(defn counter-get [key]
  (or (get @counters key) 0))

(defn counter-list []
  @counters)

(defn counter-inc! [key val]
  (swap! counters update-in [key] (fn [n] (if n (+ n val) val))))

(defn counters-reset! []
  (reset! counters (into {} (map (fn [[k _]] {k 0}) @counters))))

(comment "Aggregation config is an atom map {tenant {regex-match regex-replace ...} tenant2 {regex-match regex-replace ...}}")

(def aggregator-patterns (atom {}))

(defn get-aggregator-patterns [tenant]
  (or (get @aggregator-patterns (keyword tenant)) nil))

(defn list-aggregator-patterns []
  @aggregator-patterns)

(defn set-aggregator-patterns! [tenant pattern-map]
  (swap! aggregator-patterns update-in [(keyword tenant)] (fn [_] (convert-regex pattern-map))))

(comment "Blacklist config is an atom map {tenant [regex-match regex-match2 ...] tenant2 [regex-match regex-match2 ...}")

(def blacklist-patterns (atom {}))

(defn get-blacklist-patterns [tenant]
  (or (get @blacklist-patterns (keyword tenant)) nil))

(defn set-blacklist-patterns! [tenant pattern-list]
  (swap! blacklist-patterns update-in [(keyword tenant)] (fn [_] (for [p pattern-list] (re-pattern p)))))

(defmacro go-catch
  [& body]
  `(go
     (try
       ~@body
       (catch Exception e
         (error e (or (.getMessage e)
                      "Exception while processing channel message"))))))

(defn partition-or-time
  "Returns a channel that will either contain vectors of n items taken from ch or
   if beat-rate millis elapses then a vector with the available items. The
   final vector in the return channel may be smaller than n if ch closed before
   the vector could be completely filled."
  [n ch beat-rate buf-or-n]
  (let [out (chan buf-or-n)]
    (go (loop [arr (make-array Object n)
               idx 0
               beat (timeout beat-rate)]
          (let [[v c] (alts! [ch beat])]
            (if (= c beat)
              (do
                (if (> idx 0)
                  (do (>! out (vec (take idx arr)))
                      (recur (make-array Object n)
                             0
                             (timeout beat-rate)))
                  (recur arr idx (timeout beat-rate))))
              (if-not (nil? v)
                (do (aset ^objects arr idx v)
                    (let [new-idx (inc idx)]
                      (if (< new-idx n)
                          (recur arr new-idx beat)
                          (do (>! out (vec arr))
                              (recur (make-array Object n) 0 (timeout beat-rate))))))
                (do (when (> idx 0)
                      (let [narray (make-array Object idx)]
                        (System/arraycopy arr 0 narray 0 idx)
                        (>! out (vec narray))))
                    (close! out)))))))
    out))

(defn distinct-by
  [by coll]
  (let [step (fn step [xs seen]
               (when-let [s (seq xs)]
                 (let [f (first s)]
                   (if (seen (by f))
                     (recur (rest s) seen)
                     (cons f (lazy-seq
                              (step (rest s) (conj seen (by f)))))))))]
    (step coll #{})))

(defn now
  "Returns a unix epoch"
  []
  (quot (System/currentTimeMillis) 1000))

(defn get-min-rollup
  [rollups]
  (:min (meta rollups)))

(defn nested-select-keys
  "Nested select-keys."
  [map keyseq]
  (reduce-kv (fn [a k v]
               (merge a (if (map? v) (nested-select-keys v keyseq) {})))
             (select-keys map keyseq)
             map))

;;
;; The next section contains a series of path matching functions


(defmulti aggregate-with
  "This transforms a raw list of points according to the provided aggregation
   method. Each point is stored as a list of data points, so multiple
   methods make sense (max, min, mean). Additionally, a raw method is
   provided"
  (comp first list))

(defmethod aggregate-with :avg
  [data]
  (if (seq data)
    (/ (reduce + 0.0 data) (count data))
    data))

(defmethod aggregate-with :sum
  [data]
  (reduce + 0.0 data))

(defmethod aggregate-with :max
  [data]
  (apply max data))

(defmethod aggregate-with :min
  [data]
  (apply min data))

(defmethod aggregate-with :raw
  [data]
  data)

(defmethod aggregate-with :mean
  [data]
  (aggregate-with :avg data))
