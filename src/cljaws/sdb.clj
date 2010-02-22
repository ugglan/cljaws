(ns cljaws.sdb
  (:use (cljaws core helpers))
  (:import
   (com.xerox.amazonws.sdb Domain Item SimpleDB QueryResult
			   ItemAttribute SDBException )))

;;
;; SimpleDB
;;

(declare *sdb-service* *sdb-domain*)

(defmacro with-domain
  "Bind name as current domain. name can be keyword or string. If it
doesn't exist, it will be created when needed, but not until then."
  [name & body]
  `(binding [*sdb-domain* (.getDomain *sdb-service* (to-str ~name))]
     ~@body))

; dummy domain name due to quirk of typica library so we can select without domain
(def #^{:private true}
     DUMMY-DOMAIN "-Uncreated-Dummy-Domain-Name-For-Select-")

(defmacro with-sdb
  "Bind sbd services."
  [& body]
  `(binding [*sdb-service* (SimpleDB. *aws-id* *aws-key* true)]
     (with-domain ~DUMMY-DOMAIN
       ~@body)))

(defn current-domain []
  (let [name (.getName *sdb-domain*)]
    (if (= DUMMY-DOMAIN name)
      (throw (SDBException. "No current domain."))
      name)))

(defmacro retry-if-no-domain
  "If domain doesn't exist, create it and retry body."
  [& body]
  `(let [body# (fn [] ~@body)
	 name# (current-domain)]
     (try (body#)
	  (catch SDBException e#
	    (if (neg? (.indexOf (str e#) "The specified domain does not exist."))
	      (throw e#)
	      (do (.createDomain *sdb-service* name#)
		  (body#)))))))

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

(defn- splitter [[a b]]
  (map #(vector a %) b))

(defn- transform-attributes [attrs replace]
  (->> (map #(vector (key %) (ensure-vector (val %))) attrs)
       (mapcat splitter)
       (map #(ItemAttribute. (to-str (first %))
			     (str (second %)) replace))))


(defn batch-add-attributes
  "Batch add a map of items and attributes on the form {item {attr val}, item2 {attr val, attr2 [val1 val2]}}."
  [id-attrs-map]
  (loop [remaining id-attrs-map
	 res []]
    (let [[current remaining] (split-at 25 remaining)
	  items (into {} (map #(vector
				(to-str (first %))
				(transform-attributes (second %) false))
			      current))]
      (if (empty? items)
	(map bean res)
	(recur remaining
	       (cons (retry-if-no-domain
		      (.batchPutAttributes *sdb-domain* items)) res))))))


(defn- update-attributes [id attrs replace]
  (let [attributes (transform-attributes attrs replace)]
    (bean (retry-if-no-domain
	   (.putAttributes
	    (get-item* id) attributes)))))


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
      (bean (.deleteAttributes (get-item* id) attr-list)))))


(defn- convert-attr-list [attr-list]
  (apply merge-with #(if (vector? %1) (conj %1 %2) (vector %1 %2))
			  (map #(hash-map (keyword (.getName %))
					  (.getValue %)) attr-list)))

(defn item
  "Return a map containing the attributes of the item named id."
  [id]
  (convert-attr-list
   (retry-if-no-domain (.getAttributes (get-item* id)))))

(defn select
  "Run select with the specified query, returns a sequence of results."
  [query]
  (map #(vector (keyword (.getKey %))
		(convert-attr-list (.getValue %)))
       (.getItems (.selectItems *sdb-domain* (str "select " query) nil))))

(defn create-domain
  "Explicitly create the specified domain. Ususally you will just
use (with-domain ...) and it will be automatically created if needed."
  [name]
  (.createDomain *sdb-service* (to-str name)))

(defn delete-domain
  "Delete the current or the specified domain."
  ([] (delete-domain (current-domain)))
  ([name] (.deleteDomain *sdb-service* (to-str name))))

(defn list-domains
  "Returns a sequence of strings representing the available domains."
  []
  (map (memfn getName) (.. *sdb-service* listDomains getDomainList)))
