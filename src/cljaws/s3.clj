(ns cljaws.s3
  (:use (cljaws core helpers))
  (:import 
   (org.jets3t.service.security AWSCredentials)
   (org.jets3t.service.impl.rest.httpclient RestS3Service)
   (org.jets3t.service.model S3Object)))

;;
;; Simple Storage Service / S3
;;

(declare *s3-service*)

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

