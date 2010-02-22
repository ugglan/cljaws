(ns cljaws.sdb
  (:use [cljaws core helpers]
        [clojure.contrib.seq-utils :only [partition-all]])
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


(defn sdb-exception?
  "Is this the specified type of sdb-exception?"
  ([e] (= (class e) SDBException))
  ([e msg] (and (sdb-exception? e)
		(not (neg? (.indexOf (str e) msg))))))

(defn retry-fn-if-no-domain*
  "Create domain and retry function if current domain doesn't exist yet."
  [body-fn]
  (let [name (current-domain)]
    (loop [tries 0]
      (let [result (try (body-fn) (catch SDBException e e))]
	(cond
	 (sdb-exception? result "specified domain does not exist")
	 (do (when (zero? tries)
	       (.createDomain *sdb-service* name)
	       (Thread/sleep 200))
	     (recur (inc tries)))

	 (sdb-exception? result) (throw result)

	 :else result)))))

(defmacro retry-if-no-domain
  "If domain doesn't exist, create it and retry body."
  [& body]
  `(let [body# (fn [] ~@body)]
     (retry-fn-if-no-domain* body#)))


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


; Tools to reformat maps into something typica will understand

(defn- splitter [[a b]]
  (map #(vector a %) b))

(defn- transform-attributes
  "Takes a sequence item-maps and transforms them to individual ItemAttributes.
Example: {:a {:b 1 :c 2} :e {:f [3 4]} turns into
         {a (IA(b,1), IA(c,2)) e (IA(f,3), IA(f,4))}"
  [attrs replace]
  (->> (map #(vector (key %) (ensure-vector (val %))) attrs)
       (mapcat splitter)
       (map #(ItemAttribute. (to-str (first %))
			     (str (second %)) replace))))


; Thread-pool version of batch-put

(defn- prepare-batchput-map
  "Prepare sequence into a map of String->[ItemAttributes] that batchPutAttr wants"
  [id-attrs-seq]
  (into {} (map #(vector
		  (to-str (first %))
		  (transform-attributes (second %) false))
		id-attrs-seq)))

(declare batch-add-attributes)

(defn- batch-add-atom-seq
  "Multi batch add that will run in seperate thread."
  [id-attrs-seq all-results domain service]
  (binding [*sdb-domain* domain
	    *sdb-service* service]
    (->>
     (loop [result []]
       (let [items (prepare-batchput-map
		    (take 25 (swap! id-attrs-seq #(drop 25 %))))]
	 (if (empty? items)
	   result
	   (recur (conj result
			(retry-if-no-domain
			 (.batchPutAttributes domain items)))))))
     (map #(select-keys (bean %) [:boxUsage :requestId]))
     (swap! all-results conj))))


(defn- multi-batch-add-attributes
  "Create a threadpool to execute batch-adds in parallell."
  [n id-attrs-seq]
  (let [work-seq (atom (concat (range 25) id-attrs-seq))
	result (atom [])
	pool (java.util.concurrent.Executors/newFixedThreadPool n)
	tasks (let [[d s] [*sdb-domain* *sdb-service*]]
		(replicate n (fn [] (batch-add-atom-seq work-seq result
							d s))))]
    (doseq [future (.invokeAll pool tasks)] (.get future))
    (.shutdown pool)
    (apply concat @result)))


; Single threaded version of batch-put, will dispatch to multi-thread when needed

(defn batch-add-attributes
  "Batch add a map of items and attributes on the form {item {attr
val}, item2 {attr val, attr2 [val1 val2]}}. An optional integer argument
greater than 1 specifies the size of a thread pool that will do the work."
  ([n id-attrs-seq]
     (cond
      (= 1 n) (batch-add-attributes id-attrs-seq)
      (pos? n) (multi-batch-add-attributes n id-attrs-seq)))
  ([id-attrs-seq]
     (map #(select-keys (bean %) [:boxUsage :requestId])
	  (doall
	   (for [part (partition-all 25 id-attrs-seq)]
	     (let [items (prepare-batchput-map part)]
	       (retry-if-no-domain
		(.batchPutAttributes *sdb-domain* items))))))))


; Single-item update tools

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


; Tools to read an item

(defn- convert-attr-list [attr-list]
  (apply merge-with #(if (vector? %1) (conj %1 %2) (vector %1 %2))
			  (map #(hash-map (keyword (.getName %))
					  (.getValue %)) attr-list)))

(defn item
  "Return a map containing the attributes of the item named id."
  [id]
  (convert-attr-list
   (retry-if-no-domain (.getAttributes (get-item* id)))))


; Select

(defn- lazy-select
  ([domain query] (lazy-select domain query nil))
  ([domain query next-token]
     (let [res (.selectItems domain query next-token)
	   items (.getItems res)
	   next-token (.getNextToken res)]
       (if (nil? next-token)
	 items
	 (lazy-seq
	  (concat items
		  (lazy-select domain query next-token)))))))

(defn select
  "Run select with the specified query, returns a lazy sequence of results."
  [query]
  (map #(vector (keyword (.getKey %))
		(convert-attr-list (.getValue %)))
       (lazy-select *sdb-domain*  (str "select " query))))


; Tools to work with domains

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
