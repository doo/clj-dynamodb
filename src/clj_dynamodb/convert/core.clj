(ns clj-dynamodb.convert.core
  (:use [clojure.walk :only [walk]])
  (:require [clojure.string :as s]))

(def replace-by #'clojure.string/replace-by)

(defn to-camel-case [keyword]
  (-> (name keyword)
      (replace-by #"-(\w)" #(s/upper-case (second %)))
      (s/replace-first #"^\w" s/upper-case)))

(defn to-dashed [string]
  (-> string
      (s/replace-first #"^\w" s/lower-case)
      (replace-by #"[A-Z]" #(str "-" (s/lower-case %)))
      keyword))

(defn keyword->string [key]
  (if (keyword? key)
    (if (namespace key)
      (str (namespace key) "/" (name key))
      (name key))
    key))

(defn prewalk-except-for
  [except f form]
  (let [g (except form)]
    (if (fn? g)
      (g form)
      (walk (partial prewalk-except-for except f) identity (f form)))))

(defn map-entry-key? [form]
  (and (vector? form) (= (count form) 2)))

(defn map-entry-key [form]
  (when (map-entry-key? form)
    (first form)))

(defn convert-body [key? convert-key except body]
  (prewalk-except-for
   (fn [form]
     (let [c (except form)]
       (when (fn? c)
         (fn [form]
           (let [[key value] form]
             [(convert-key key) (c value)])))))
   (fn [form] (if (and (map-entry-key? form)
                       (key? (first form)))
                (let [[key value] form]
                  [(convert-key key) value])
                form))
   body))
