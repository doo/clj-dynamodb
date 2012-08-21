(ns clj-dynamodb.convert.to-dynamodb
  (:use clj-dynamodb.convert.core))

(defn string-set? [x]
  (and (set? x) (every? string? x)))

(defn number-set? [x]
  (and (set? x) (every? number? x)))

(def type-checks
  [["S" #'string?]
   ["N" #'number?]
   ["SS" #'string-set?]
   ["NS" #'number-set?]
   [:unknown (constantly true)]])

(defn to-dynamodb-number-set [x]
  (set (map str x)))

(defn unknown-error [x]
  (throw (Exception. (str "can not determine the type of value: " (pr-str x)))))

(def type-conversions
  {"S" #'identity
   "N" #'str
   "SS" #'identity
   "NS" #'to-dynamodb-number-set
   :unknown #'unknown-error})

(defn value-type [type-checks value]
  (some (fn [[type type-check]]
          (when (type-check value)
            type)) type-checks))

(defn convert-value [type-checks type-conversions value]
  (let [type (value-type type-checks value)
        c (get type-conversions type)]
    {type (c value)}))

(defn convert-map [type-checks type-conversions m]
  (into {} (map (fn [[k v]]
                  [(keyword->string k)
                   (convert-value type-checks type-conversions v)]) m)))

(defn prepare-fn [type-checks type-conversions keys-to-convert]
  (let [cm (partial convert-map type-checks type-conversions)
        cv (partial convert-value type-checks type-conversions)
        keys-to-convert (set keys-to-convert)]
    (fn [form]
      (when-let [key (map-entry-key form)]
        (when (keys-to-convert key)
          (let [value (second form)]
            (if (map? value) cm cv)))))))