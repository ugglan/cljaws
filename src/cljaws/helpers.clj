(ns cljaws.helpers)

(defmacro while-or-timeout 
  "Repeat body until it returns a value that satisfies pred. Returns
false if no value satisfies pred within timeout seconds."
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