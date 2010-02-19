(ns cljaws.helpers)

(defmacro while-or-timeout 

  "Repeats body until it returns a value that satisfies pred. Returns
false if no value satisfies pred within timeout seconds.
First retry will happen after 0.5s, each succeeding retry will happen 
after twice as long up to a maximum of 32s, except for the last retry 
which will happen at the timeout-time, but not earlier than 0.5s after 
the previous attempt."

  [pred timeout & body]

  `(loop [end-time# (+ (System/currentTimeMillis) (* 1000 ~timeout))
	  time-skip# 500]
     (let [value# (do ~@body)
	   now# (System/currentTimeMillis)]
       (if (not (~pred value#)) 
	 value#
	 (and 
	  (< now# end-time#)
	  (do
	    (let [sleep-time# 
		  (max 500 
		       (min time-skip# 
			    (- end-time# now#)))]
	      (Thread/sleep sleep-time#)
	      (recur end-time#
		     (if (< time-skip# 29999)
		       (* 2 time-skip#) 
		       time-skip#)))))))))