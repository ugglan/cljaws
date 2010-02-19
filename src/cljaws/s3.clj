(ns cljaws.s3
  (:use (cljaws core helpers))
  (:import 
   (org.jets3t.service.security AWSCredentials)
   (org.jets3t.service.impl.rest.httpclient RestS3Service)
   (org.jets3t.service.model S3Object)
   (org.jets3t.service.utils ServiceUtils)))

;;
;; Simple Storage Service / S3
;;

(declare *s3-service* *s3-bucket*)

(defmacro with-s3 
  [& body]
  `(binding [*s3-service* (RestS3Service. (AWSCredentials. *aws-id* *aws-key*))]
     ~@body))

(defn list-buckets 
  []
  (map (memfn getName) (.listAllBuckets *s3-service*)))

;

(defmacro with-bucket 
  [bucket & body]
  `(binding [*s3-bucket* (.getOrCreateBucket *s3-service* ~bucket)]
     ~@body))

(
defn delete-bucket []
  (.deleteBucket *s3-service* *s3-bucket*))


(defn list-bucket []
  (let [contents (.listObjects *s3-service* *s3-bucket*)]
    (map bean contents)))


(defn put-object [key obj]
  (cond 
   (string? obj) (.putObject *s3-service* *s3-bucket* (S3Object. key obj)))
  )

(defn get-object-details [key]
  (bean (.getObjectDetails *s3-service* *s3-bucket* key)))


(defn get-object [key]
  (.getObject *s3-service* *s3-bucket* key))

(defn get-object-as-stream [key]
  (.getDataInputStream (get-object key)))

(defn get-object-as-string 
  "Get object as utf-8 encoded string, probably not a good idea on big objects."
  [key]
  (let [baos (java.io.ByteArrayOutputStream.)
	is (get-object-as-stream key)]
    (loop [b (.read is)]
      (if (neg? b)
	(String. (.toByteArray baos) "UTF-8")
	(do (.write baos b)
	    (recur (.read is)))))))

(defn delete-object [key]
  (.deleteObject *s3-service* *s3-bucket* key))