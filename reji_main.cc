#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>
#include <string.h>

#include "json.h"
#include "reji_schema.h"

extern "C" {
#include "../redismodule.h"

//============================================================
int RejiCreate_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
	RedisModule_AutoMemory(ctx);

	if (argc != 2)
		return RedisModule_WrongArity(ctx);

	size_t json_data_len = 0;
	const char *json_data = RedisModule_StringPtrLen(argv[1], &json_data_len);
	reji_index_t *index = NULL;

	RedisModule_Log(ctx, "notice", "REJI: create index json: %*.*s", json_data_len, json_data_len, json_data);
	int res = reji_index_create(json_data, json_data_len, &index);
 
	if(res == SCHEMA_OK && index)
	{
		RedisModuleString *index_key_string = (RedisModuleString *)RedisModule_CreateStringPrintf(ctx, REDIS_SCHEMA_KEY);
		RedisModuleKey *redis_key = (RedisModuleKey *)RedisModule_OpenKey(ctx, index_key_string, REDISMODULE_READ | REDISMODULE_WRITE);
		RedisModuleString *name = (RedisModuleString *)RedisModule_CreateStringPrintf(ctx, index->name);
		RedisModule_HashSet(redis_key, REDISMODULE_HASH_NONE, name, argv[1], NULL);
		RedisModule_CloseKey(redis_key);
		return RedisModule_ReplyWithSimpleString(ctx, "OK");
	}
	
	return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
}

//============================================================
int RejiDrop_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
	RedisModule_AutoMemory(ctx);

	if (argc != 2)
		return RedisModule_WrongArity(ctx);

	size_t len = 0;
	const char *name = RedisModule_StringPtrLen(argv[1], &len);
	
	RedisModule_Log(ctx, "notice", "REJI: drop index: %s", name);
	int res = reji_index_drop((char*)name);

	if(res == SCHEMA_OK)
	{
		RedisModuleString *index_key_string = (RedisModuleString *)RedisModule_CreateStringPrintf(ctx, REDIS_SCHEMA_KEY);
		RedisModuleKey *redis_key = (RedisModuleKey *)RedisModule_OpenKey(ctx, index_key_string, REDISMODULE_READ | REDISMODULE_WRITE);
		RedisModuleString *rname = (RedisModuleString *)RedisModule_CreateStringPrintf(ctx, name);
		
		RedisModule_HashSet(redis_key, REDISMODULE_HASH_NONE, rname, REDISMODULE_HASH_DELETE, NULL);
		RedisModule_CloseKey(redis_key);
		return RedisModule_ReplyWithSimpleString(ctx, "OK");
	}
	
	return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
}


/* REJI.PUT <key> <json_object> -- adds a record to Redis store and indexes it */
int RejiPut_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
	RedisModule_AutoMemory(ctx);

	if (argc != 3)
		return RedisModule_WrongArity(ctx);

	// try to parse the record first
	struct json_tokener* tok = json_tokener_new();
	json_tokener_set_flags(tok, 0);
	
	size_t json_data_len = 0;
	const char *json_data = RedisModule_StringPtrLen(argv[2], &json_data_len);
	struct json_object *jobj = json_tokener_parse_ex(tok, json_data, json_data_len);

	// index by "name" field
	if(jobj)
	{
		json_object_object_foreach(jobj, key, val)
		{
			if(strcmp(key, "name") == 0)
			{
				RedisModuleString *index_key_string = RedisModule_CreateStringPrintf(ctx, "REJI:IDX:%s", json_object_to_json_string(val));
				RedisModuleKey *redis_key = RedisModule_OpenKey(ctx, index_key_string, REDISMODULE_READ | REDISMODULE_WRITE);
				RedisModule_StringSet(redis_key, argv[1]);
				RedisModule_CloseKey(redis_key);
				
				return RedisModule_ReplyWithSimpleString(ctx, "OK");
			}
		}
		
		return RedisModule_ReplyWithLongLong(ctx, rand());
	}
	
	
	
	return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
}

/* This function must be present on each Redis module. It is used in order to
 * register the commands into the Redis server. */
int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);

    if(RedisModule_Init(ctx, "reji", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;
	
    if(RedisModule_CreateCommand(ctx, "reji.put", RejiPut_RedisCommand, "", 0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

	if(RedisModule_CreateCommand(ctx, "reji.create", RejiCreate_RedisCommand, "", 0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

	if(RedisModule_CreateCommand(ctx, "reji.drop", RejiDrop_RedisCommand, "", 0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

	reji_schema_init();
	RedisModule_Log(ctx, "notice", "REJI loaded");
    return REDISMODULE_OK;
}

// extern "C"
}
