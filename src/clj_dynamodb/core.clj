(ns clj-dynamodb.core
  (:use [clj-dynamodb.convert.core :only [to-camel-case to-dashed convert-body keyword->string]])
  (:require [clj-dynamodb.convert.to-dynamodb :as dyn]
            [clj-dynamodb.convert.to-clojure :as clj]
            [cheshire.core :as json]
            [aws-signature-v4.signing :as signature]))

(def ^:dynamic dynamodb-api-version "DynamoDB_20111205")

(def default-content-type "application/x-amz-json-1.0")

(def default-headers {"Content-Type" default-content-type})

(defn amz-target
  "Returns an appropriate x-amz-target header value for the given
   target. Example:

   (amz-target :get-item)
   > \"DynamoDB_20111205.GetItem\""
  [target]
  {:pre [(keyword? target)]}
  (str dynamodb-api-version "." (to-camel-case target)))

(def basic-request
  {:request-method :post
   :scheme "https"
   :server-port 443
   :uri "/"
   :headers default-headers})

(defn add-aws [req aws-params]
  (assoc req :aws aws-params))

(defn use-endpoint [req]
  (assoc req :server-name (get-in req [:aws :endpoint])))

(defn add-target [req target]
  (assoc-in req [:headers "x-amz-target"] (amz-target target)))

(def ^:dynamic value-keys #{:item :hash-key-element :range-key-element :id})

(defn to-dynamodb [body]
  (convert-body keyword?
                to-camel-case
                (dyn/prepare-fn
                 dyn/type-checks
                 dyn/type-conversions
                 value-keys)
                body))

(defn to-dynamodb-request-body [req]
  (update-in req [:body] to-dynamodb))

(defn default-prepare-request [req aws-params]
  (-> req
      to-dynamodb-request-body
      (update-in [:body] json/generate-string)
      (add-aws aws-params)
      use-endpoint
      signature/sign-request))

(defn to-clojure [body]
  (convert-body string? to-dashed (clj/prepare-fn clj/type-conversions #{"Item"}) body))

(defn to-clojure-response-body [req]
  (update-in req [:body] to-clojure))

(defn parse-body [request-or-response]
  (update-in request-or-response [:body]
             #(json/parse-string (String. %))))

(defn default-prepare-response [response]
  (-> response
      parse-body
      to-clojure-response-body))

(defn client-error-response? [resp]
  (= (:status resp) 400))

(def provisioned-throughput-exceeded-error "com.amazonaws.dynamodb.v20111205#ProvisionedThroughputExceededException")

(defn provisioned-throughput-exceeded-error-response? [resp]
  (and (client-error-response? resp)
       (= (get-in resp [:body "__type"]) provisioned-throughput-exceeded-error)))

(defn server-error-response? [resp]
  (= (:status resp) 500))

(defn ok? [resp]
  (= (:status resp) 200))

(defn request-with-exponential-backoff [client max-number-of-retries req]
  (loop [r 0]
    (let [resp (client req)]        
      (if (and (or (server-error-response? resp)
                   (and (client-error-response? resp)                        
                        (provisioned-throughput-exceeded-error-response?
                         (parse-body resp))))
               (< r max-number-of-retries))
        (do
          (Thread/sleep (* (long (Math/pow 2 r)) 50))
          (recur (inc r)))
        (assoc-in resp [:aws :request-retries] r)))))

(defn wrap-client-with-exponential-backoff [client max-number-of-retries]
  (fn [req]
    (request-with-exponential-backoff client max-number-of-retries req)))

(defn basic-get-item-request [table-name hash-key]
  (let [body {:table-name table-name
              :key {:hash-key-element hash-key}}]
    (-> basic-request
        (assoc :body body)
        (add-target :get-item))))

(defn basic-put-item-request [table-name item]
  (-> basic-request
      (assoc :body {:table-name (keyword->string table-name)
                    :item item})
      (add-target :put-item)))

;; batch

(defn batch-put-request [item & [id]]
  (let [r {:put-request {:item item}}]
    (if id
      (assoc-in r [:put-request :id] id)
      r)))

(defn basic-batch-write-item-request [batch-request-items]
  (-> basic-request
      (assoc :body {:request-items batch-request-items})
      (add-target :batch-write-item)))

(defn batch-write-items [client prepare-request batch-write-item-request]
  (loop [n 1
         request (prepare-request batch-write-item-request)]
    (let [response (client request)
          unprocessed-items (get (:body (parse-body response)) "UnprocessedItems")]  
      (if (or (empty? unprocessed-items))
        (assoc-in response [:aws :batch-cycles] n)
        (recur (inc n) (prepare-request (basic-batch-write-item-request unprocessed-items)))))))