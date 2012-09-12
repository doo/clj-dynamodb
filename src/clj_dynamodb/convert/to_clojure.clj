(ns clj-dynamodb.convert.to-clojure
  (:use clj-dynamodb.convert.core))

(def type-conversions
  {"S" #'identity
   "N" #'bigdec
   "SS" #'set
   "NS" #(set (map bigdec %))})

(defn convert-value [type-conversions value]
  (let [[type v] (first value)
        c (get type-conversions type)]
    (c v)))

(defn convert-map [type-conversions m]
  (into {} (map (fn [[k v]]
                  [(to-dashed k) (convert-value type-conversions v)]) m)))

(defn prepare-fn [type-conversions maps-to-convert & [values-to-convert]]
  (let [convert-map (partial convert-map type-conversions)
        cm (fn [value] (if (sequential? value)
                         (map convert-map value)
                         (convert-map value)))
        cv (partial convert-value type-conversions)
        maps-to-convert (set maps-to-convert)
        values-to-convert (set values-to-convert)]
    (fn [form]
      (when-let [key (map-entry-key form)]
        (cond
         (maps-to-convert key) cm
         (values-to-convert key) cv)))))
