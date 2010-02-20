(ns cljaws.core)

(declare *aws-id* *aws-key*)
;;
;; Generic aws
;;

(def *aws-properties-file* "aws.properties")

(defmacro with-aws-keys [id key & body]
  `(binding [*aws-id* ~id
	     *aws-key* ~key]
     ~@body))

(defn- read-properties [file-name]
  (into {} 
	(doto (java.util.Properties.)
	  (.load (java.io.FileInputStream. file-name)))))
	
(defmacro with-aws
  "Load credentials and setup specfied services."
  {:arglists '([service?+  & body])}  
  [& body]
  (let [{:strs [id key]} (read-properties *aws-properties-file*)
	[withs body] (split-with #(contains? #{'s3 'ec2 'sdb 'sqs} %) body)
	body (loop [res `(do ~@body)
		    w (reverse withs)]
	       (if (empty? w) res
		   (recur (list (symbol (str "with-" (first w))) res)
			  (next w))))]
    `(with-aws-keys ~id ~key ~body)))

