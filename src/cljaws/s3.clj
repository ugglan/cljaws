(ns cljaws.s3
  (:use (cljaws core helpers))
  (:import
   (org.jets3t.service.security AWSCredentials)
   (org.jets3t.service.impl.rest.httpclient RestS3Service)
   (org.jets3t.service.model S3Object)
   (org.jets3t.service.utils ServiceUtils)
   (org.jets3t.service.acl GroupGrantee Permission GrantAndPermission)
   (org.jets3t.service S3ServiceException)))

;;
;; Simple Storage Service / S3
;;

(declare *s3-service* *s3-bucket*)

(defmacro with-s3
  "Bind s3 service."
  [& body]
  `(binding [*s3-service* (RestS3Service. (AWSCredentials. *aws-id* *aws-key*))]
     ~@body))

(defn list-buckets
  "Returns a sequence of the names of all your buckets as strings."
  []
  (map (memfn getName) (.listAllBuckets *s3-service*)))

;

(defmacro with-bucket
  "Bind existing bucket or create it if it doesn't exist. bucket can
be a string or a keyword."
  [bucket & body]
  `(binding [*s3-bucket* (.getOrCreateBucket *s3-service* (to-str ~bucket))]
     ~@body))

(defn delete-bucket
  "Delete the current bucket."
  []
  (.deleteBucket *s3-service* *s3-bucket*))


(defn list-bucket
  "Returns a sequence of all the available content in the current bucket."
  []
  (let [contents (.listObjects *s3-service* *s3-bucket*)]
    (map bean contents)))

(declare bucket-acl*)
(defn- put-object*
  "Update s3-object with acl of parent bucket and then put it."
  [obj]
  (.setAcl obj (bucket-acl*))
  (.putObject *s3-service* *s3-bucket* obj))

(defn put-object
  "Put a key-object pair into the active bucket. Object can be a
string or a File. The put object will inherit the access control list
of the bucket."
  [key obj]
  (let [key (to-str key)]
    (cond
     (string? obj) (put-object* (S3Object. key obj))
     (= java.io.File
	(class obj)) (put-object* (doto (S3Object. obj) (.setKey key)))
	:else (throw (IllegalArgumentException.
		      (str "Can't put object of type " (class obj)))))))

(defn get-object-details
  "Return a map with info about the object or nil if not available."
  [key]
  (try (bean (.getObjectDetails *s3-service* *s3-bucket* (to-str key)))
       (catch S3ServiceException e nil)))

(defn get-object*
  "Returns an jets3t S3Object or nil if not available."
  [key]
  (try (.getObject *s3-service* *s3-bucket* (to-str key))
       (catch S3ServiceException e nil)))

(defn get-object-as-stream
  "Returns an input stream for the object stored at key or nil if not available."
  [key]
  (when-let [obj (get-object* key)]
    (.getDataInputStream obj)))


; This is just a quick hack to replace the jets3t helper function. As it
; uses readLine it will add an unwanted newline at the end of the string.
;
(defn get-object-as-string
  "Get object as utf-8 encoded string, probably not a good idea on big objects."
  [key]
  (when-let [is (get-object-as-stream key)]
    (let [baos (java.io.ByteArrayOutputStream.)
	  input (take-while (comp not neg?) (repeatedly #(.read is)))]
      (doseq [b input] (.write baos b))
      (String. (.toByteArray baos) "UTF-8"))))

(defn delete-object
  "Delete the object from the current bucket."
  [key]
  (.deleteObject *s3-service* *s3-bucket* (to-str key)))


; access control

(defn bucket-acl*
  "Access control object of this bucket"
  [] (.getBucketAcl *s3-service* *s3-bucket*))


(def #^{:private true}
     parse-grantee
     {:all-users GroupGrantee/ALL_USERS
      :authenticated-users GroupGrantee/AUTHENTICATED_USERS
      :log-delivery GroupGrantee/LOG_DELIVERY})

(def #^{:private true}
     parse-permission
     {:full-control Permission/PERMISSION_FULL_CONTROL
      :read Permission/PERMISSION_READ
      :read-acp Permission/PERMISSION_READ_ACP
      :write Permission/PERMISSION_WRITE
      :write-acp Permission/PERMISSION_WRITE_ACP
      :revoke-all :revoke-all})

(defn- make-grant [[grantee permission]]
  (let [g (parse-grantee grantee)
	p (parse-permission permission)]
    (if (nil? g) (throw (IllegalArgumentException.
			 (str "Unknown grantee: " grantee))))
    (if (nil? p) (throw (IllegalArgumentException.
			 (str "Unknown permission: " permission))))
    (if (keyword? p)
      {p g}
      (GrantAndPermission. g p))))

(defn- update-acl [acl grants]
  (let [parsed (map make-grant grants)
	grant-set (into	#{} (filter (comp not map?) parsed))
	revoke-set (->> (filter map? parsed)
			(map :revoke-all)
			(filter (comp not nil?))
			(into #{}))]
    (doseq [grantee revoke-set] (.revokeAllPermissions acl grantee))
    (doto acl (.grantAllPermissions grant-set))))


(defn grant
  "Grant the permissions in the map grants to the current bucket or a
key within the bucket. Any :revoke-all permissions will be executed
before any grants, so {:all-users :read :all-users :revoke-all} is the
same as {:all-users :revoke-all :all-users :read}

Example: (grant {:all-users :read})
         (grant \"mykey\" {:authenticated-users :revoke-all}) "

  ([grants]
     (let [acl (bucket-acl*)]
       (.setAcl *s3-bucket* (update-acl acl grants)))
     (.putBucketAcl *s3-service* *s3-bucket* ))

  ([key grants]
     (when-let [obj (get-object* key)]
       (let [acl (.getObjectAcl *s3-service* *s3-bucket* (to-str key))]
	 (.setAcl obj (update-acl acl grants)))
       (.putObject *s3-service* *s3-bucket* obj))))