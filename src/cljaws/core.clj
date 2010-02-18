(ns cljaws.core
  (:import 
   (com.xerox.amazonws.sqs2 QueueService MessageQueue Message SQSUtils)
   (com.xerox.amazonws.ec2 Jec2 ImageDescription)
   (com.xerox.amazonws.sdb Domain Item SimpleDB QueryResult ItemAttribute)
   
   (org.jets3t.service.security AWSCredentials)
   (org.jets3t.service.impl.rest.httpclient RestS3Service)
   (org.jets3t.service.model S3Object)))

(declare *sqs-queue*
	 *aws-id*
	 *aws-key*
	 *s3-service*
	 *sdb-service*
	 *sdb-domain*)


;;
;; Generic aws
;;

(defmacro with-aws-keys [id key & body]
  `(binding [*aws-id* ~id
	     *aws-key* ~key]
     ~@body))


(defn- read-properties [file-name]
  (into {} 
	(doto (java.util.Properties.)
	  (.load (java.io.FileInputStream. file-name)))))
	
(defmacro with-aws [& body]
  (let [{id "id" key "key"} (read-properties "aws.properties")]
    `(with-aws-keys ~id ~key ~@body)))


;;
;; SimpleDB
;;

(defn- to-str 
  "turn keyword/symbol into string without prepending : or ', or just pass through if already string"
  [s] (if (string? s) 
	s
	(name s)))  

(defmacro with-domain [name & body]
  `(binding [*sdb-domain* (.getDomain *sdb-service* (to-str ~name))]
     ~@body))

; dummy domain name due to quirk of typica library so we can select without domain
(defmacro with-sdb
  [& body]
  `(binding [*sdb-service* (SimpleDB. *aws-id* *aws-key* true)]
     (with-domain :-Uncreated-Dummy-Domain-Name-For-Select-
       ~@body)))

(defn ensure-vector [v]
  (if (vector? v) v (vector v)))

(defn- get-item [id]
  (.getItem *sdb-domain* (to-str id)))

(defn delete-item [id]
  (.deleteItem *sdb-domain* (to-str id)))

(defn- update-attributes [id attrs replace]
  (.putAttributes 
   (get-item id)
   (map #(ItemAttribute. (to-str (first %)) (str (second %)) replace) attrs)))

(defn add-attributes
  [id attrs] (update-attributes id attrs false))

(defn replace-attributes 
  [id attrs] (update-attributes id attrs true))

(defn delete-attributes [id attrs]
  (let [attr-list    
	(cond 
	 (vector? attrs) (map #(ItemAttribute. (to-str %) nil true) attrs)
	 (map? attrs) (map #(ItemAttribute. (to-str (first %)) nil true) attrs)
	 :else (list (ItemAttribute. (to-str attrs) nil true)))]
    (if (not-empty attr-list)
      (.deleteAttributes (get-item id) attr-list))))


(defn- convert-attr-list [attr-list]
  (apply merge-with #(if (vector? %1) (conj %1 %2) (vector %1 %2))
			  (map #(hash-map (keyword (.getName %)) 
					  (.getValue %)) attr-list))) 

(defn item [id] 
  (convert-attr-list (.getAttributes (get-item id))))

(defn select
  [query] 
  (map #(vector (keyword (.getKey %)) 
		(convert-attr-list (.getValue %)))
       (.getItems (.selectItems *sdb-domain* (str "select " query) nil))))


(defn create-domain [name]
  (.createDomain *sdb-service* (to-str name)))

(defn delete-domain [name]
  (.deleteDomain *sdb-service* (to-str name)))

(defn list-domains []
  (map (memfn getName) (.. *sdb-service* listDomains getDomainList)))

;;
;; Simple Storage Service / S3
;;

(defmacro with-s3 
  [& body]
  `(binding [*s3-service* (RestS3Service. (AWSCredentials. *aws-id* *aws-key*))]
     ~@body))

(defn create-bucket [bucket]
  (.createBucket *s3-service* bucket))

(defn delete-bucket [bucket]
  (.deleteBucket *s3-service* bucket))

(defn list-buckets 
  []
  (map (memfn getName) (.listAllBuckets *s3-service*)))


(defn list-bucket [bucket]
  (let [contents (.listObjects *s3-service* (.getBucket *s3-service* bucket))]
    (map bean contents)))


;;
;; Elastic cloud / EC2
;;


(defn list-ec2-images [] 
  (let [ec2 (Jec2. *aws-id* *aws-key*)]
    (.describeImages ec2 '())))

;; 
;; Simple Queue Servie / SQS
;;

(defmacro with-sqs-queue [name & body]
  `(binding [*sqs-queue* (.getOrCreateMessageQueue (QueueService. *aws-id* *aws-key* true) ~name)]
     ~@body))

(defn list-sqs-queues 
  ([] (list-sqs-queues nil))
  ([prefix] 
     (map #(subs % (inc (.lastIndexOf % "/")))
	  (map (comp str (memfn getUrl))
	       (seq (.listMessageQueues (QueueService. *aws-id* *aws-key* true) prefix))))))


(defn delete-queue 
  []
  (.deleteQueue *sqs-queue*))


(defn enqueue 
  [message]
  (.sendMessage *sqs-queue* message))


(defmacro do-until-timeout [timeout & body]
  `(loop [end-time# (+ (System/currentTimeMillis) (* 1000 ~timeout))
	  time-skip# 500]
     (let [value# (do ~@body)
	   now# (System/currentTimeMillis)]
       (or value#
	   (if (> now# end-time#)
	     nil
	     (do 
	       (let [sleep-time# (max 500 
				      (min time-skip# 
					   (- end-time# now#)))]
		 (println "sleep " sleep-time#)
		 (Thread/sleep sleep-time#)
		 (recur end-time#
			(if (< time-skip# 29999) 
			  (* 2 time-skip#) 
			  time-skip#)))))))))
  
(defn dequeue 
  ([] (dequeue (* 365 24 3600 1000))) ;; wait a year as default
  ([seconds]
     (let [msg (do-until-timeout
		(* 1000 seconds)
		(.receiveMessage *sqs-queue*))]
       (if msg (do
		 (.deleteMessage *sqs-queue* msg)
		 (.getMessageBody msg))))))

