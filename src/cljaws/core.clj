(ns cljaws.core)

(declare *aws-id* *aws-key*)


;;
;; Common

(defn to-str 
  "turn keyword/symbol into string without prepending : or ', or
just pass through if already string"
  [s] (if (string? s) 
	s
	(name s)))  

;;
;; Generic aws
;;

(def #^{:doc "File name to read AWS keys from." }
     *aws-properties-file* "aws.properties")

(defmacro with-aws-keys 
  "Setup aws with specified credentials, doesn't take service args. 
To use services use individual (with-sdb ...) etc."
  [id key & body]
  `(binding [*aws-id* ~id
	     *aws-key* ~key]
     ~@body))

(defn- read-properties [file-name]
  (into {} 
	(doto (java.util.Properties.)
	  (.load (java.io.FileInputStream. file-name)))))
	
(defmacro with-aws
  "Load credentials and setup specfied services. 
Example: (with-aws :s3 :sdb ...)"
  {:arglists '([service?+  & body])}  
  [& body]
  (let [{:strs [id key]} (read-properties *aws-properties-file*)
	[services body] (split-with keyword? body)
	body (reduce #(list (symbol (str "with-" (name %2))) %1)
		     (cons `(do ~@body) services))]
    `(with-aws-keys ~id ~key ~body)))

