(ns cljaws.core-test
  (:use (cljaws core helpers) :reload-all)
  (:use [clojure.test]))


(defn make-queue-name []
  (str "__CLJAWS_test-queue-" (rand-int 100000000)))

(defmacro queue-test-wrapper [& body]
  `(let [queue# (make-queue-name)]
     (with-aws (with-sqs-queue queue#
		 (let [msg# (do ~@body)]
		   (delete-queue)
		   msg#)))))


(deftest test-queues
  (is (= "Hello world!"
	 (queue-test-wrapper 
	  (enqueue "Hello world!")
	  (dequeue)))
      "Fetch message from queue")
  
  (is (false?
       (queue-test-wrapper 
	(dequeue 5)))
      "Fetch nothing (timeout) from empty queue"))


(deftest test-queue-create-delete
  (let [queues (with-aws (list-sqs-queues))
	names (take 10 (repeatedly make-queue-name))]
    
    (is (< (count queues)
	   (with-aws 
	     (doall
	      (for [i (range (count names))]
		(let [q (nth names i)]
		  (with-sqs-queue q
		    (enqueue (str "Hello " i q))
		    ))))
	     (count (list-sqs-queues))))
	"Only verifies that at least 1 queue has been created while loading 10 queues with messages")
    
    (is (let [osize (count queues)]
	  (not (nil? 
		(with-aws
		  (while-or-timeout 
		   #(< % 10) 5
		   (- (count (list-sqs-queues))
		      osize))))))
	"Give list-sqs-queues 5 seconds to return a queuelist with 10 more queues")

    (with-aws 
      (doall
       (for [i (range (count names))]
	 (let [q (nth names i)]
	   (with-sqs-queue q 
	     (is (= (str "Hello " i q)
		    (dequeue 10))
		 "Verify that we get one message from each queue within 10 secs")
	     (delete-queue))))))
    
    (is (not (false?
	      (with-aws 
		(while-or-timeout 
		 #(= queues %) 5 
		 (list-sqs-queues)))))
	"Give list-sqs-queues 5 seconds to return the original queuelist")))

