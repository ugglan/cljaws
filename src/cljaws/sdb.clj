(ns cljaws.sdb
  (:use (cljaws core helpers))
  (:import 
   (com.xerox.amazonws.sdb Domain Item SimpleDB QueryResult
			   ItemAttribute)))

;;
;; SimpleDB
;;

(declare *sdb-service* *sdb-domain*)


(defmacro with-domain 
  "Bind existing domain as domain to work with. name can be keyword or string."
  [name & body]
  `(binding [*sdb-domain* (.getDomain *sdb-service* (to-str ~name))]
     ~@body))

; dummy domain name due to quirk of typica library so we can select without domain
(defmacro with-sdb
  "Bind sbd services."
  [& body]
  `(binding [*sdb-service* (SimpleDB. *aws-id* *aws-key* true)]
     (with-domain :-Uncreated-Dummy-Domain-Name-For-Select-
       ~@body)))

(defn ensure-vector 
  "Make sure v is a vector. If it isn't, wrap it in one."
  [v]
  (if (vector? v) v (vector v)))

(defn- get-item* [id]
  (.getItem *sdb-domain* (to-str id)))

(defn delete-item 
  "Delete item from current domain. id can be a keyword or a string."
  [id]
  (.deleteItem *sdb-domain* (to-str id)))

(defn- update-attributes [id attrs replace]
  (.putAttributes 
   (get-item* id)
   (map #(ItemAttribute. (to-str (first %)) (str (second %)) replace) attrs)))

(defn add-attributes
  "Add all attributes in the map attrs to the item id."
  [id attrs] (update-attributes id attrs false))

(defn replace-attributes 
  "Add or replace all attributes in the map attrs in the item id."
  [id attrs] (update-attributes id attrs true))

(defn delete-attributes 
  "Delete all the attributes in attrs in item id. attrs can be a vector, a map, a keyword or a string."
  [id attrs]
  (let [attr-list    
	(cond 
	 (vector? attrs) (map #(ItemAttribute. (to-str %) nil true) attrs)
	 (map? attrs) (map #(ItemAttribute. (to-str (first %)) nil true) attrs)
	 :else (list (ItemAttribute. (to-str attrs) nil true)))]
    (if (not-empty attr-list)
      (.deleteAttributes (get-item* id) attr-list))))


(defn- convert-attr-list [attr-list]
  (apply merge-with #(if (vector? %1) (conj %1 %2) (vector %1 %2))
			  (map #(hash-map (keyword (.getName %)) 
					  (.getValue %)) attr-list))) 

(defn item 
  "Return a map containing the attributes of the item named id."
  [id] 
  (convert-attr-list (.getAttributes (get-item* id))))

(defn select
  "Run select with the specified query, returns a sequence of results."
  [query] 
  (map #(vector (keyword (.getKey %)) 
		(convert-attr-list (.getValue %)))
       (.getItems (.selectItems *sdb-domain* (str "select " query) nil))))

(defn create-domain
  "Create the specified domain."
  [name]
  (.createDomain *sdb-service* (to-str name)))

(defn delete-domain 
  "Delete the specified domain."
  [name]
  (.deleteDomain *sdb-service* (to-str name)))

(defn list-domains 
  "Returns a sequence of strings representing the available domains."
  []
  (map (memfn getName) (.. *sdb-service* listDomains getDomainList)))

