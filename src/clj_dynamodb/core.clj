(ns clj-dynamodb.core
  (:use [clj-dynamodb.convert.core :only [to-camel-case to-dashed convert-body keyword->string]])
  (:require [clj-dynamodb.convert.to-dynamodb :as dyn]
            [clj-dynamodb.convert.to-clojure :as clj]
            [cheshire.core :as json]
            [aws-signature-v4.signing :as signature]
            [lamina.core.result :as r]
            [lamina.core :as l]
            [aleph.http :as a])
  (:import org.jboss.netty.buffer.ChannelBufferInputStream))

(def ^:dynamic dynamodb-api-version "DynamoDB_20111205")

(def ^:dynamic dynamodb-batch-size 25)

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

(defn retry-request? [response]
  (or (server-error-response? response)
      (and (client-error-response? response)
           (provisioned-throughput-exceeded-error-response?
            (parse-body response)))))

(defn exponential-backoff [n]
  (* (long (Math/pow 2 n)) 50))

(defn remove-host-header [request]
  (update-in request [:headers] dissoc "host"))

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

(defn handle-error-response [response]
  (if (or (client-error-response? response)
            (server-error-response? response))
    (throw (Exception. (String. (:body response))))
    response))

;;TODO: use slingshot here (client error und server error exception)


;; convert body

(defn slurp-channel-buffer
  "Converts an org.jboss.netty.buffer.ChannelBuffer into a string."
  [channel-buffer]
  (slurp (ChannelBufferInputStream. channel-buffer)))

(defn body-channel-as-string
  "Converts a closed channel filled with org.jboss.netty.buffer.ChannelBuffer(s) into
   a string."
  [channel]
  (reduce
   (fn [r s] (str r (slurp-channel-buffer s)))
   "" (l/channel-seq channel)))

(defn body-to-string
  "Converts the body of an Aleph HTTP response map into a string. The response
   is either an org.jboss.netty.buffer.ChannelBuffer or a channel filled with
   ChannelBuffer(s) (in the case of a HTTP chunked transfer encoding)"
  [req]
  (update-in
   req [:body]
   (fn [body]
     (if (l/channel? body)
       (body-channel-as-string body)
       (slurp-channel-buffer body)))))

(defn delay-request [request]
  (if-let [wait (get-in request [:aws :delay])]
    (r/timed-result wait request)
    request))

(def http-request
  (l/pipeline
   remove-host-header
   delay-request
   a/http-request
   body-to-string))

(def http-execute-with-exponential-backoff
  (l/pipeline
   (fn [request] (l/merge-results request (http-request request)))
   (fn [[request response]]
     (let [aws (:aws request)
           n (:request-retries aws 0)
           max-number-of-retries (:max-number-of-retries aws -1)]
       (if (and (retry-request? response)
                (or (= max-number-of-retries -1)
                    (< n max-number-of-retries)))
         (let [eb (exponential-backoff n)
               request (update-in request [:aws] assoc
                                  :delay eb
                                  :request-retries (inc n))]
           ;;(println "restart" n "in" eb) ;; TODO: replace with lamina trace
           (l/restart request))
         (assoc-in response [:aws :request-retries] n))))))

(defn batch-write-items [prepare-request batch-write-item-request]
  (l/run-pipeline
   batch-write-item-request
   (fn [request] (prepare-request request))
   http-execute-with-exponential-backoff
   (fn [response]
     (let [response (handle-error-response response)
           unprocessed-items (get (json/parse-string (:body response))
                                  "UnprocessedItems")]
       ;;(println "unprocessed" unprocessed-items) ;;TODO replace through lamina trace
       (if (empty? unprocessed-items)
         response
         (l/restart (basic-batch-write-item-request unprocessed-items)))))))

(defn group-batch-items-by-table [batch-items]
  (into {}
        (map (fn [[k v]] [k (map #(dissoc % :table) v)])
             (group-by :table batch-items))))

(defn prepare-batch-items [batch-items]
  (->> batch-items
       (sort-by :table)
       (partition-all dynamodb-batch-size)
       (map group-batch-items-by-table)))

(defn mass-batch-write-items [prepare-request batch-items]
  (let [batches (prepare-batch-items batch-items)]
    (apply l/merge-results
           (map (fn [batch]
                  (let [req (basic-batch-write-item-request batch)]
                    (batch-write-items
                     prepare-request
                     req)))
                batches))))