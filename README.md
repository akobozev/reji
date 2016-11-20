REdis Json Index
--------------

Reji is the module allowing to index records consisting of JSON objects.
It provides the following Redis commands:

* REJI.CREATE - Creates single/compound unique/nonunique index for specified JSON fields
* REJI.DROP - Drops index and removes all the index references
* REJI.PUT - Adds a new record into Redis store and indexes it
* REJI.KEYS - Retrieves the list of keys associated with index record
* REJI.GET - Retrieves the list of data records associated with index record
* REJI.DEL - Finds and deletes a JSON record(s) by index
* REJI.KDEL - Deletes a JSON record by key and removes the index reference

Building Reji Module
--------------

1. Make sure autoconf and libtool packages are installed
2. Run 'make'
