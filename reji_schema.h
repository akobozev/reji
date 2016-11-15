#ifndef _REJI_SCHEMA_H
#define _REJI_SCHEMA_H

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <vector>

#include "json.h"

// redis schema key
#define REDIS_SCHEMA_KEY "REJI:SCHEMA"
 
// index flags 
#define REJI_INDEX_UNIQUE 	0x1
#define REJI_INDEX_CASE		0x2

// schema status codes
#define SCHEMA_OK 0
#define SCHEMA_FAIL 1
#define SCHEMA_INDEX_EXISTS 2
#define SCHEMA_INDEX_NOT_EXISTS 3
 
typedef struct _reji_index
{
	char *name;
	short flags;
	short numColumns;
	char **columns;
} reji_index_t;

typedef struct {
	reji_index_t *index;
	char *key;
} reji_index_key_t;

typedef std::vector<reji_index_key_t> reji_index_keys_list_t;

typedef struct _reji_index_iter
{
	reji_index_t *index;
} reji_index_iter_t;
	
void reji_schema_init();
void reji_schema_fini();

int reji_index_create(const char *json_data, size_t json_data_len, reji_index_t **outIndex);
int reji_index_drop(char *indexName);

int reji_index_get(char *indexName, reji_index_t **index);
int reji_index_iter_start(reji_index_iter_t **iter);
void reji_index_iter_stop(reji_index_iter_t *iter);
int reji_index_iter_next(reji_index_iter_t *iter);

int reji_build_index_keys(json_object *jobj, reji_index_keys_list_t &keys_list);

#endif //  _REJI_SCHEMA_H
