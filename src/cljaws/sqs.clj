(ns cljaws.sqs
  (:use (cljaws core helpers))
  (:import 
   (com.xerox.amazonws.sqs2 QueueService MessageQueue Message
			    SQSUtils)))

;; 
;; Simple Queue Service / SQS
;;

(declare *sqs-queue*
	 *sqs-service*)

(defmacro with-sqs 
  "Bind SQS service."
  [& body]
  `(binding [*sqs-service* (QueueService. *aws-id* *aws-key* true)]
     ~@body))

(defmacro with-queue 
  "Bind the specified queue as current. queue can be keyword or string."
  [name & body]
  `(binding [*sqs-queue* (.getOrCreateMessageQueue *sqs-service* (to-str ~name))]
     ~@body))

(defn list-queues 
  "Return a sequence of strings representing all available queues, or queues
with a name starting with prefix."
  ([] (list-queues nil))
  ([prefix] 
     (map #(subs % (inc (.lastIndexOf % "/")))
	  (map (comp str (memfn getUrl))
	       (seq (.listMessageQueues (QueueService. *aws-id* *aws-key* true) prefix))))))


(defn delete-queue 
  "Delete the current queue."
  []
  (.deleteQueue *sqs-queue*))


(defn enqueue 
  "Enqueue string message in current queue."
  [message]
  (.sendMessage *sqs-queue* message))


(defn dequeue 
  "Dequeue a message, return false if timeout. If no timeout is specified, 
it will block until a message is available."
  ([] (dequeue (* 365 24 3600 1000))) ;; wait a year as default, sortof blocking
  ([seconds]
     (let [msg (while-or-timeout 
		nil? seconds
		(.receiveMessage *sqs-queue*))]
       (and (not (false? msg)) 
	    (do
	      (.deleteMessage *sqs-queue* msg)
	      (.getMessageBody msg))))))

