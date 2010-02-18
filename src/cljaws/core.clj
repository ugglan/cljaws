(ns cljaws.core)

(declare *aws-id* *aws-key*)
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

