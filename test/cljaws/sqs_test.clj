(ns cljaws.sqs-test
  (:use (cljaws core sqs helpers core-test) :reload-all)
  (:use [clojure.test]))


(defmacro queue-test-wrapper [& body]
  `(let [queue# (make-unique-name "queue")]
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
  (let [nr 5
	queues (with-aws (list-sqs-queues))
	names (take nr (repeatedly #(make-unique-name "queue")))]
    
    ;; setup queues and fill with one msg each
    (with-aws 
      (doall
       (for [i (range (count names))]
	 (let [q (nth names i)]
	   (with-sqs-queue q
	     (enqueue (str "Hello " i q))
	     )))))

    
    (is (not (nil?
	      (with-aws
		(while-or-timeout 
		 #(< % nr) 5
		 (- (count (list-sqs-queues))
		    (count queues))))))
	"Give list-sqs-queues 5 seconds to return a queuelist with correct nr of queues")
    
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
		 #(not= (count queues) (count %)) 10 
		 (list-sqs-queues)))))
	"Give list-sqs-queues 10 seconds to return the original queuelist")))

