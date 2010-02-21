
# cljaws

cljaws is a relatively thin wrapper for the typica and jets3t AWS
libraries. In its current state it is very sparse and incomplete,
functionality will be added on a "need it!"-basis.

My aim is to have something that makes it easy and fun (for me!) to
use all of the Amazon Web Services in a consistent and somewhat
clojuresque manner.

I have no ambitions to completely hide the innards of the parent
libraries because sometimes I might want to work with the
Java-objects.

The API will very likely keep changing for some time.


## Installation

cljaws uses the leiningen build tool, [get it](http://github.com/technomancy/leiningen) 
if you don't have it!


1. Clone the repo

2. Fetch dependencies - `lein deps`

3. Copy the aws.properties.template file to aws.properties and
   edit it to contain your own credentials.

As an alternative to creating aws.properties you can use
`(with-aws-keys ID KEY ....)` but I don't recommended it.

Optional 4. Run `lein test` Note: as AWS doesn't guarantee how long it takes
for modifications to be available, some test may fail occasionally.


## Usage 

I'm trying to keep the test-cases pretty complete, but they might not
be very readable so here is some example usage:


    ; enqueue the names of all my s3-buckets as a message in queue "bucketlist"
    (use '(cljaws core s3 sqs))
    
    (with-aws :sqs
      (with-queue :bucketlist
        (doseq [bucket-name (with-s3 (list-buckets))]
    	   (enqueue bucket-name))))
    
    
    (use '(cljaws sdb helpers))
    
    ; create sdb domain
    (with-aws :sdb (create-domain :workspace))
    
    ; dequeue messages sent above and add each one as a row in the sdb-domain
    (with-aws :sdb :sqs
      (with-queue :bucketlist 
    	(with-domain :workspace
    	  (doseq [msg (take-while (comp not false?) (repeatedly #(dequeue 5)))]
    		 (add-attributes msg {:type "testing bucket"})))))
    
    ; select all rows and attibutes from domain
    (with-aws :sdb (select "* from workspace"))
    
    
    ; Upload an image and a string to mybucket. Then make the string
    ; publicly available on http://mybucket.s3.amazonaws.com/foo.txt
    (use 'cljaws.s3)
    (with-aws :s3
      (with-bucket :mybucket 
        (put-object "secretfile.jpg" (java.io.File. "cute.jpg"))
        (put-object "foo.txt" "hello world!")
        (grant "foo.txt" {:all-users :read})))
    


## License

 Copyright (c) 2010 Tobias Löfgren (contact@tobiaslofgren.com)

 MIT-style license:

 Permission is hereby granted, free of charge, to any person
 obtaining a copy of this software and associated documentation
 files (the "Software"), to deal in the Software without
 restriction, including wit hout limitation the rights to use,
 copy, modify, merge, publish, distribute, sublicense, and/or sell
 copies of the Software, and to permit persons to whom the
 Software is furnished to do so, subject to the following
 conditions:

 The above copyright notice and this permission notice shall be
 included in all copies or substantial portions of the Software.

 THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
 OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 OTHER DEALINGS IN THE SOFTWARE.

