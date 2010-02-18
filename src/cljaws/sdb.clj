(ns cljaws.sdb
  (:use (cljaws core helpers))
  (:import 
   (com.xerox.amazonws.sdb Domain Item SimpleDB QueryResult
			   ItemAttribute)))

;;
;; SimpleDB
;;

(declare *sdb-service* *sdb-domain*)

(defn to-str 
  "turn keyword/symbol into string without prepending : or ', or
just pass through if already string"
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

