#ifndef _REJI_SCHEMA_H
#define _REJI_SCHEMA_H

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>

#ifdef __cplusplus
extern "C" {
#endif

// redis schema key
#define REDIS_SCHEMA_KEY "REJI:SCHEMA"
 
// index flags 
#define REJI_INDEX_UNIQUE 	0x1
#define REJI_INDEX_CASE		0x2

typedef struct _reji_index
{
	char *name;
	short flags;
	short numColumns;
	char **columns;
} reji_index_t;

typedef struct _reji_index_iter
{
	reji_index_t *index;
	void *iter;
} reji_index_iter_t;
	
void reji_schema_init(char *indexes);
void reji_schema_fini();

int reji_index_create(const char *json_data, size_t json_data_len, reji_index_t **outIndex);
int reji_index_drop(char *indexName);

void reji_index_get(char *indexName, reji_index_t *index);
void reji_index_iter_start(reji_index_iter_t *iter);
void reji_index_iter_stop(reji_index_iter_t *iter);
void reji_index_iter_next(reji_index_iter_t *iter);

#ifdef __cplusplus
}
#endif

#endif //  _REJI_SCHEMA_H
