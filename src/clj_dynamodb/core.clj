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

(def ^:dynamic dynamodb-batch-write-size 25)

(def ^:dynamic dynamodb-batch-get-size 100)

(def ^:dynamic default-content-type "application/x-amz-json-1.0")

(def ^:dynamic default-headers {"Content-Type" default-content-type})

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

(defn table-key [hash-key & [range-key]]
  (let [m {:hash-key-element hash-key}]
    (if range-key
      (assoc m :range-key-element range-key)
      m)))

(defn batch-get-request
  ([table-name keys]
     {table-name
      {:keys keys}})
  ([table-name keys attributes-to-get]
     (assoc (batch-get-request table-name keys)
       :attributes-to-get attributes-to-get)))

(defn basic-batch-write-item-request [batch-request-items]
  (-> basic-request
      (assoc :body {:request-items batch-request-items})
      (add-target :batch-write-item)))

(defn basic-batch-get-item-request [batch-request-items]
  (-> basic-request
      (assoc :body {:request-items batch-request-items})
      (add-target :batch-get-item)))

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

(defn body-to-string
  "Converts the body of an Aleph HTTP response map into a string. The response
   is either an org.jboss.netty.buffer.ChannelBuffer or a channel filled with
   ChannelBuffer(s) (in the case of a HTTP chunked transfer encoding)"
  [req]
  (update-in
   req
   [:body]
   (fn [body]
     (if (sequential? body)
       (reduce (fn [b cb] (str b (slurp-channel-buffer cb))) "" body)
       (slurp-channel-buffer body)))))

(defn delay-request [request]
  (if-let [wait (get-in request [:aws :delay])]
    (r/timed-result wait request)
    request))

(defn handle-chunked-transfer-encoding
  "If chunked transfer encoding is used to transfer the response body, then
   the :body entry in the map will be a lamina channel. In this case this
   function takes care that the full body is transfered before the subsequent
   pipeline stages continues to process the response.
   It is very important that a separate nested pipeline is used here to consume
   the full body. If this is done in the pipeline that handles the request and
   response cycle, this will cause the whole pipeline to block forever.
   The appropriate strategy to handle this situation has been found in the
   aleph.http.core/decode-message function."
  [{:keys [content-type character-encoding body] :as response}]
  (if (l/channel? body)
    (l/run-pipeline
     (l/reduce* conj [] body)
     ;; Another important point is that the body must be assigned in the next stage
     ;; otherwise the channel will not be realized (or block each other), when the
     ;; result is assigned as :body to the request. Here run-pipeline takes care that
     ;; the full channel has been consumed by reduce* / reduced before it is assigned
     ;; to the request map.
     #(assoc response :body %))
    response))

(def http-request
  (l/pipeline
   remove-host-header
   delay-request
   a/http-request
   handle-chunked-transfer-encoding
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

(defn batch-pipeline [prepare-request]
  (l/pipeline
   (fn [request] (prepare-request request))
   http-execute-with-exponential-backoff))

(defn repeat-unprocessed-pipeline [extract-unprocessed create-repeat-request]
  (fn [response]
    (let [response (handle-error-response response)
          body (json/parse-string (:body response))
          unprocessed (extract-unprocessed body)]
       ;;(println "unprocessed" unprocessed-items) ;;TODO replace through lamina trace
       (if (empty? unprocessed)
         response
         (l/restart (create-repeat-request unprocessed))))))

(defn batch-write-items [prepare-request batch-write-item-request]
  (l/run-pipeline
   batch-write-item-request
   (batch-pipeline prepare-request)
   (repeat-unprocessed-pipeline (fn [body] (get body "UnprocessedItems"))
                                basic-batch-write-item-request)))

(defn batch-get-items [prepare-request batch-get-item-request]
  (l/run-pipeline
   batch-get-item-request
   (batch-pipeline prepare-request)
   (repeat-unprocessed-pipeline (fn [body] (get body "UnprocessedKeys"))
                                basic-batch-get-item-request)))

(defn group-batch-items-by-table [batch-items]
  (into {}
        (map (fn [[k v]] [k (map #(dissoc % :table) v)])
             (group-by :table batch-items))))

(defn prepare-batch-items [batch-items]
  (->> batch-items
       (sort-by :table)
       (partition-all dynamodb-batch-write-size)
       (map group-batch-items-by-table)))

(defn mass-batch-execute [batches execute-batch]
  (apply l/merge-results
         (map (fn [batch]
                (execute-batch batch)) batches)))

(defn mass-batch-write-items [prepare-request batch-items]
  (mass-batch-execute (prepare-batch-items batch-items)
                      (fn [batch]
                        (batch-write-items
                         prepare-request
                         (basic-batch-write-item-request batch)))))

(defn mass-batch-get-items [prepare-request table-name keys]
  (mass-batch-execute (partition-all dynamodb-batch-get-size keys)
                      (fn [keys]
                        (batch-get-items
                         prepare-request
                         (basic-batch-get-item-request
                          (batch-get-request table-name keys))))))