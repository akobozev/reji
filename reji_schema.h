#ifndef _REJI_SCHEMA_H
#define _REJI_SCHEMA_H

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <vector>
#include <string>
#include <unordered_map>
#include "json.h"

// redis schema key
#define REDIS_REJI_PREFIX "REJI"
#define REDIS_SCHEMA_KEY "REJI:SCHEMA"
#define REDIS_INDEX_KEY_PREFIX "REJI:KEY:"
#define REDIS_INDEX_KEY_SEPARATOR "/"
 
// index flags 
#define REJI_INDEX_UNIQUE 	0x1
#define REJI_INDEX_CASE		0x2

#define IS_UNIQUE(x) (x->flags & REJI_INDEX_UNIQUE)

// schema status codes
#define SCHEMA_OK 0
#define SCHEMA_FAIL 1
#define SCHEMA_INDEX_EXISTS 2
#define SCHEMA_INDEX_NOT_EXISTS 3

// index status codes
#define INDEX_OK 0
#define INDEX_FAIL 1

typedef struct _reji_index
{
	char *name;
	short flags;
	short numColumns;
	char **columns;
} reji_index_t;

typedef struct
{
	reji_index_t *index;
	const char *key;
} reji_index_key_t;

typedef std::unordered_map<std::string, reji_index_key_t> reji_index_keys_map_t;

typedef struct _reji_index_iter
{
	reji_index_t *index;
} reji_index_iter_t;

typedef struct _reji_index_val
{
	std::string col;
	const char *val;
	size_t val_len;
} reji_index_val_t;
	
typedef std::unordered_map<std::string, reji_index_val_t*> IndexValMap;

void reji_schema_init();
void reji_schema_fini();

int reji_index_create(const char *json_data, size_t json_data_len, reji_index_t **outIndex);
int reji_index_drop(char *indexName);

int reji_index_get(const char *indexName, size_t len, reji_index_t **index);
int reji_index_iter_start(reji_index_iter_t **iter);
void reji_index_iter_stop(reji_index_iter_t *iter);
int reji_index_iter_next(reji_index_iter_t *iter);

bool reji_build_key(reji_index_t *index, IndexValMap& obj_map, std::string& key);
bool reji_build_key_from_record(reji_index_t *index, json_object *jobj, reji_index_key_t& index_key);
int reji_build_index_keys(json_object *jobj, reji_index_keys_map_t& keys_map);
void reji_free_index_keys(reji_index_keys_map_t& keys_map);
void reji_free_index_key(reji_index_key_t& index_key);

char *reji_str_to_lower(char *src);
void reji_string_to_lower(std::string& src);

#endif //  _REJI_SCHEMA_H
