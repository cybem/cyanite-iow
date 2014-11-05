(ns org.spootnik.cyanite.rollup
  "Implements rollup finders for different strategies."
  (:require [org.spootnik.cyanite.util :refer [now]]
            [clojure.tools.logging :refer [info error debug]]))

(defprotocol RollupFinder
  (find-rollup [this from to]))

(defn get-within-fn
  [from]
  (fn [{:keys [rollup period] :as rollup-def}]
    (and (>= from (- (now) (* rollup period)))
         rollup-def)))

(defn precise-rollup-finder
  "Find most precise storage period given the oldest point wanted"
  [{:keys [rollups]}]
  (info "creating precise rollup finder")
  (info "rollups:" rollups)
  (reify
    RollupFinder
    (find-rollup
      [this from to]
      (let [within-fn (get-within-fn from)]
        (some within-fn (sort-by :rollup rollups))))))

(defn resolution-rollup-finder
  "Find most suitable rollup for given resolution"
  [{:keys [rollups resolution] :or {resolution 800}}]
  (info "creating resolution rollup finder")
  (info "rollups:" rollups)
  (info "resolution:" resolution)
  (reify
    RollupFinder
    (find-rollup
      [this from to]
      (let [within-fn (get-within-fn from)
            sorted-rollups (sort-by :rollup rollups)
            within (filter within-fn sorted-rollups)
            resolution-fn (fn [{:keys [rollup period] :as rollup-def}]
                            (>= (/ (- to from) rollup) resolution))]
        (and within (or (some resolution-fn (rseq within)) (first within)))))))
