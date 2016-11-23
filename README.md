# REdis Json Index

Reji is the module allowing to index records consisting of JSON objects. Indexing is based on values of root level JSON object fields. 
It provides the following Redis commands:

* REJI.CREATE - Creates single/compound unique/nonunique index for specified JSON fields
* REJI.DROP - Drops index and removes all the index references
* REJI.PUT - Adds a new record into Redis store and indexes it
* REJI.PUTNX - Adds a new record into Redis store, indexes it or fails if data key exists (same as SETNX)
* REJI.KEYS - Retrieves the list of keys associated with index record
* REJI.GET - Retrieves the list of data records associated with index record
* REJI.DEL - Finds and deletes a JSON record(s) by index
* REJI.KDEL - Deletes a JSON record by key and removes the index reference

# Building Reji Module

1. Make sure autoconf, libtool and libstdc++ packages are installed
2. Run 'make'

# Commands reference

## `REJI.CREATE`

    REJI.CREATE <index_definition_json>
	
Creates new index based on the definition given in JOSN format. Indexes all the existing records.
If the existing record is not in JSON format or doesn't contain the index columns, record will be ignored.
If record violates unique index constaint, record will be ignored. 

Returns the list of keys that violated the unique index constaint.

Index definition JSON object contents:

    {
	    name: <index_name>,
		columns: [<column1 name>, ..., <columnN name>],
		unique: <true|false>
	}

Redis-cli examples:

    > REJI.CREATE "{\"name\": \"idx\", \"columns\": [\"author\"]}"
    > (empty list or set)

    > REJI.CREATE "{\"name\": \"uidx\", \"columns\": [\"author\"], \"unique\": \"true\"}"
    > 1) book1

## `REJI.DROP`

    REJI.DROP <index_name>

Drops the existing index and removes all the index related records leaving only the orinial data records.
Returns OK or "(error) Index not found"

Redis-cli examples:

    > REJI.DROP uidx
	> OK


## `REJI.PUT`

    REJI.PUT <key> <json_data>

Inserts and indexes a new record with given key and value. Ignores the indexing part if either unable to parse the JSON data or can't find the index columns (data is inserted to DB in any case).
Returns OK or "(error) Unique index violation: <index_name>".

Redis-cli examples:

    > REJI.PUT book1 "{\"author\": \"Corets, Eva (Jr.)\", \"name\": \"Maeve Ascendant - Part II\", \"price\": \"16.99\"}"
	> OK
    > REJI.PUT book2 "{\"author\": \"Corets, Eva (Jr.)\", \"name\": \"Maeve Ascendant - Part II\", \"price\": \"16.99\"}"
	> (error) Unique index violation: uidx
    > REJI.PUT not_a_book_data "some_data" 
	> OK

## `REJI.PUTNX`

    REJI.PUTNX <key> <json_data>

See REJI.PUT for reference.
Fails if given key exists in DB (same as SETNX)

## `REJI.KEYS`

    REJI.KEYS <index_name> <column1_name> <value1> ... <columnN_name> <valueN>

Retrieves the list of keys corresponding to non-unique index or list containing single key for unique index.
Parameters are given in column/value pair for all the columns defined in index.



	
