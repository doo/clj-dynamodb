(ns clj-dynamodb.connection-pool
  (:require lamina.connections
            aleph.http
            clojure.java.data)
  (:import org.apache.commons.pool.BasePoolableObjectFactory
           org.apache.commons.pool.impl.GenericObjectPool
           org.apache.commons.pool.impl.GenericObjectPool$Config))

(defn poolable-object-factory [options]
  (let [{:keys [make-object destroy-object]} options]
    (proxy [BasePoolableObjectFactory] []
      (destroyObject [obj]
        (destroy-object obj))
      (makeObject []
        (make-object)))))

(defn http-client-factory [base-request]
  (poolable-object-factory
   {:make-object (fn [] (aleph.http/http-client base-request))
    :destroy-object (fn [obj] (lamina.connections/close-connection obj))}))

(defonce http-client-connection-pools (atom {}))

(defn close-http-client-conntection-pools []
  (let [pools (map val @http-client-connection-pools)]
    (doseq [pool pools]
      (.close pool))
    (reset! http-client-connection-pools {})))

(defn- set-fields [object field-value-pairs]
  (let [class (class object)]
    (doseq [[field value] field-value-pairs]
      (let [class-field (.getField class (name field))]
        (.set class-field
              object
              (clojure.java.data/to-java
               (.getType class-field)
               value))))))

(def ^:private http-connection-pool-default-config
  {:maxActive 100
   :maxIdle 100
   :timeBetweenEvictionRunsMillis 5000
   :minEvictableIdleTimeMillis 20000
   :numTestsPerEvictionRun 100})

(defn create-http-connection-pool [request]
  (let [endpoint (get-in request [:aws :endpoint])
        http-client-connection-pool-config (get-in request [:aws :connection-pool-config])
        config-object (GenericObjectPool$Config.)
        config (merge http-connection-pool-default-config http-client-connection-pool-config)]
    (set-fields config-object config)
    (GenericObjectPool.
     (http-client-factory
      (-> (select-keys request [:scheme :server-port])
          (assoc :server-name endpoint)))
     config-object)))

(defn get-http-client-connection-pool [request]
  (let [endpoint (get-in request [:aws :endpoint])
        http-client-connection-pool (get @http-client-connection-pools endpoint)]
    (if-not http-client-connection-pool
      (let [pool (create-http-connection-pool request)]
        (get
         (swap! http-client-connection-pools
                assoc endpoint pool)
         endpoint))
      http-client-connection-pool)))