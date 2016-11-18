#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>
#include <string.h>

#include <string>
#include <vector>

#include "json.h"
#include "reji_schema.h"

static bool g_indexes_loaded = false;

#define CHECK_SCHEMA_LOADED(ctx) \
    { \
        if(!g_indexes_loaded) { \
            if(RejiLoadIndexes(ctx)) RedisModule_ReplyWithError(ctx, "Failed to load indexes"); \
            g_indexes_loaded = true;\
        } \
    }

extern "C" {
#include "../redismodule.h"

//============================================================
void PrintArgs(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    for(int i = 0; i < argc; i++)
    {
        size_t len = 0;
        const char *data = RedisModule_StringPtrLen(argv[i], &len);
        RedisModule_Log(ctx, "notice", "argv[%d]: %*.*s", i, len, len, data);
    }
}

//============================================================
void DeleteAllKeys(RedisModuleCtx *ctx, char *indexName)
{
    RedisModule_Log(ctx, "notice", "Delete index keys: start");
    int count = 0;
    RedisModuleCallReply *reply = NULL;
    RedisModuleString *match = (RedisModuleString *)RedisModule_CreateStringPrintf(ctx, "%s/%s*", REDIS_INDEX_KEY_PREFIX, indexName);
    size_t cursor0_len = 1;
    const char *cursor0 = "0";
    RedisModuleString *cursor = (RedisModuleString *)RedisModule_CreateStringPrintf(ctx, "0");

    int done = 0;

    do
    {
        RedisModule_Log(ctx, "notice", "Delete index keys: cursor: %*.*s", cursor0_len, cursor0_len, cursor0);
        reply = RedisModule_Call(ctx, "scan", "scs", cursor, "match", match);
    
        if(!reply)
            break;

        size_t reply_len = RedisModule_CallReplyLength(reply);

        if(reply_len == 2)
        {
            RedisModuleCallReply *c = RedisModule_CallReplyArrayElement(reply, 0);
            RedisModuleCallReply *list = RedisModule_CallReplyArrayElement(reply, 1);

            cursor = RedisModule_CreateStringFromCallReply(c);
            
            cursor0 = RedisModule_CallReplyStringPtr(c, &cursor0_len);

            size_t list_len = RedisModule_CallReplyLength(list);
            for(size_t i = 0; i < list_len; i++)
            {
                RedisModuleCallReply *rkey = RedisModule_CallReplyArrayElement(list, i);
                RedisModuleString *key = RedisModule_CreateStringFromCallReply(rkey);
                RedisModuleKey *redis_key = (RedisModuleKey *)RedisModule_OpenKey(ctx, key, REDISMODULE_WRITE);
                RedisModule_DeleteKey(redis_key);
                RedisModule_CloseKey(redis_key);
                count++;
            }
        }

        RedisModule_FreeCallReply(reply);
        reply = NULL;
        
        if(cursor0_len == 1 && cursor0[0] == '0')
            done = 1;
    }
    while(!done);

    RedisModule_Log(ctx, "notice", "Delete index keys: end[%d]", count);
}

//============================================================
int RejiLoadIndexes(RedisModuleCtx *ctx)
{
    RedisModule_Log(ctx, "notice", "Start load indexes");
    RedisModuleString *index_key_string = (RedisModuleString *)RedisModule_CreateStringPrintf(ctx, REDIS_SCHEMA_KEY);
    RedisModuleCallReply *reply = RedisModule_Call(ctx, "hgetall", "s", index_key_string);
    
    if(!reply)
        return 0;

    size_t reply_len = RedisModule_CallReplyLength(reply);
    RedisModule_Log(ctx, "notice", "Num of indexes: %d", reply_len/2);
    size_t i = 0;
    for(; i < reply_len; i += 2)
    {
        RedisModuleCallReply *elem_key = RedisModule_CallReplyArrayElement(reply, i);
        RedisModuleCallReply *elem_data = RedisModule_CallReplyArrayElement(reply, i+1);

        size_t elem_key_len = 0;
        size_t elem_data_len = 0;
        const char *elem_key_str = RedisModule_CallReplyStringPtr(elem_key, &elem_key_len);
        const char *elem_data_str = RedisModule_CallReplyStringPtr(elem_data, &elem_data_len);

        reji_index_t *index = NULL;
        int res = reji_index_create(elem_data_str, elem_data_len, &index);
        
        if(res == SCHEMA_OK && index)
        {
            RedisModule_Log(ctx, "notice", "Loaded index[%*.*s]: %*.*s",
                            elem_key_len, elem_key_len, elem_key_str, 
                            elem_data_len, elem_data_len, elem_data_str);
        }
        else
        {
            RedisModule_Log(ctx, "notice", "Index already loaded: %*.*s",
                            elem_key_len, elem_key_len, elem_key_str);
        }
        
    }
    
    RedisModule_FreeCallReply(reply);
	return 0;
}

//============================================================
struct json_object *RejiParseJSON(const char *data, size_t len)
{
	struct json_tokener* tok = json_tokener_new();
	json_tokener_set_flags(tok, 0);
	
	struct json_object *jobj = json_tokener_parse_ex(tok, data, len);
	json_tokener_free(tok);

	return jobj;
}
	
//============================================================
int RejiPut(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, bool nx)
{
	RedisModule_AutoMemory(ctx);

	if (argc != 3)
		return RedisModule_WrongArity(ctx);

    CHECK_SCHEMA_LOADED(ctx);

	// try to parse the record first
	size_t json_data_len = 0;
	const char *json_data = RedisModule_StringPtrLen(argv[2], &json_data_len);
	struct json_object *jobj = RejiParseJSON(json_data,json_data_len);

	int ret_code = REDISMODULE_OK;
	
	do
	{
		// index the record if json parsing succeeded
		if(jobj)
		{
			std::vector<RedisModuleKey *> records_list;
			reji_index_keys_map_t keys_map;
			reji_index_keys_map_t existing_keys_map;

			bool update_mode = false;
			
			reji_build_index_keys(jobj, keys_map);

			// try to insert the user data first
			RedisModuleKey *user_data_redis_key = (RedisModuleKey *)RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);
			if(RedisModule_KeyType(user_data_redis_key) == REDISMODULE_KEYTYPE_EMPTY)
			{
				RedisModule_StringSet(user_data_redis_key, argv[2]);
				records_list.push_back(user_data_redis_key);
			}
			// fail on existing user key in NX mode
			else if(nx)
			{
				// user key exists - bail out
				RedisModule_ReplyWithError(ctx, "Key is not unique");
				RedisModule_CloseKey(user_data_redis_key);

				ret_code = REDISMODULE_ERR;
			}
			// update record: parse existing record and build keys for further processing
			else if(RedisModule_KeyType(user_data_redis_key) == REDISMODULE_KEYTYPE_STRING)
			{
				size_t existing_json_data_len = 0;
				char *existing_json_data = RedisModule_StringDMA(user_data_redis_key, &existing_json_data_len, REDISMODULE_READ);
				struct json_object *existing_jobj = RejiParseJSON(existing_json_data, existing_json_data_len);

				if(existing_jobj)
				{
					reji_build_index_keys(existing_jobj, existing_keys_map);
					update_mode = true;
				}
				else
				{
					RedisModule_ReplyWithError(ctx, "Can't update record: existing data is not in JSON format");
					RedisModule_CloseKey(user_data_redis_key);

					ret_code = REDISMODULE_ERR;
				}
			}
			// fail on non-string data
			else
			{
				RedisModule_ReplyWithError(ctx, "Can't update record: existing data is not a string");
				RedisModule_CloseKey(user_data_redis_key);

				ret_code = REDISMODULE_ERR;
			}
			
			for(reji_index_keys_map_t::iterator keys_it = keys_map.begin(); ret_code == REDISMODULE_OK && keys_it != keys_map.end(); ++keys_it)
			{
				reji_index_key_t &index_key = keys_it->second;

				// ignore existing keys while updating record
				if(update_mode)
				{
					reji_index_keys_map_t::iterator existing_key_it = existing_keys_map.find(index_key.key);
					if(existing_key_it != existing_keys_map.end())
					{
						// remove existing key from map for further insertion
						reji_index_key_t &key = existing_key_it->second;
						existing_keys_map.erase(existing_key_it);
						reji_free_index_key(key);
						
						continue;
					}
				}
				
				RedisModuleString *key_string = RedisModule_CreateString(ctx, index_key.key, strlen(index_key.key));
				RedisModuleKey *redis_key = (RedisModuleKey *)RedisModule_OpenKey(ctx, key_string, REDISMODULE_READ | REDISMODULE_WRITE);
				if(RedisModule_KeyType(redis_key) == REDISMODULE_KEYTYPE_EMPTY)
				{
					RedisModule_StringSet(redis_key, argv[1]);
					records_list.push_back(redis_key);
				}
				else
				{
					// key exists - unique index violation
					std::string error_str("Unique index violation: ");
					error_str += index_key.index->name;

					RedisModule_ReplyWithError(ctx, error_str.c_str());
					RedisModule_CloseKey(redis_key);

					ret_code = REDISMODULE_ERR;
				}
			}

			// delete the index keys in case of violation and close them
			for(std::vector<RedisModuleKey *>::iterator key_it = records_list.begin(); key_it != records_list.end(); ++key_it)
			{
				if(ret_code == REDISMODULE_ERR)
					RedisModule_DeleteKey(*key_it);

				RedisModule_CloseKey(*key_it);
			}

			reji_free_index_keys(keys_map);

			if(update_mode)
			{
				// update user record if no violation detected and delete existing keys that are not relevant after update
				if(ret_code == REDISMODULE_OK)
				{
					RedisModule_StringSet(user_data_redis_key, argv[2]);
					RedisModule_CloseKey(user_data_redis_key);

					for(reji_index_keys_map_t::iterator keys_it = keys_map.begin(); keys_it != keys_map.end(); ++keys_it)
					{
						reji_index_key_t &index_key = keys_it->second;
						RedisModuleString *key_string = RedisModule_CreateString(ctx, index_key.key, strlen(index_key.key));
						RedisModuleKey *redis_key = (RedisModuleKey *)RedisModule_OpenKey(ctx, key_string, REDISMODULE_READ | REDISMODULE_WRITE);

						RedisModule_DeleteKey(redis_key);
						RedisModule_CloseKey(redis_key);
					}
				}

				reji_free_index_keys(existing_keys_map);
			}
		}
	} while(0);	

	// cleanup
	if(jobj)
		json_object_put(jobj);
	
	return ret_code == REDISMODULE_OK ? RedisModule_ReplyWithSimpleString(ctx, "OK") : REDISMODULE_OK;
}

//============================================================
int RejiGetKeysAndValues(bool keysOnly, RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
	RedisModule_AutoMemory(ctx);

	if (argc < 4 || (argc%2) != 0)
		return RedisModule_WrongArity(ctx);

    CHECK_SCHEMA_LOADED(ctx);

	size_t name_len = 0;
	const char *name = RedisModule_StringPtrLen(argv[1], &name_len);

	IndexValMap val_map;
	reji_index_t *index = NULL;
	int res = reji_index_get(name, name_len, &index);

	if(res != SCHEMA_OK)
		return RedisModule_ReplyWithError(ctx, "Index not found");

	// build value map
	size_t col_len = 0;
	const char *col = NULL;
	
	reji_index_val_t *col_val_arr = (reji_index_val_t*)calloc(argc-2, sizeof(reji_index_val_t));
	for(int i = 2, j = 0; i < argc; i += 2, j++)
	{
		col = RedisModule_StringPtrLen(argv[i], &col_len);
		col_val_arr[j].val = RedisModule_StringPtrLen(argv[i+1], &col_val_arr[j].val_len);
		col_val_arr[j].col.assign(col, col_len);
		reji_string_to_lower(col_val_arr[j].col);
		val_map[col_val_arr[j].col] = &col_val_arr[j];
		/*
		RedisModule_Log(ctx, "notice", "REJI: get key column: %s, value: %*.*s", 
						col_val_arr[j].col.c_str(), 
						col_val_arr[j].val_len,
						col_val_arr[j].val_len,
						col_val_arr[j].val);
		*/
	}

	std::string key("");
	if(!reji_build_key(index, val_map, key))
		return RedisModule_ReplyWithError(ctx, "Not enough columns to build key");

	// fetch key and values
	RedisModuleString *key_string = RedisModule_CreateString(ctx, key.c_str(), key.length());
	RedisModuleKey *redis_key = (RedisModuleKey *)RedisModule_OpenKey(ctx, key_string, REDISMODULE_READ);
	int key_type = RedisModule_KeyType(redis_key);
	long arr_len = 0;
	if(key_type == REDISMODULE_KEYTYPE_STRING)
	{
		size_t pk_len = 0;
		const char *pk_data = RedisModule_StringDMA(redis_key, &pk_len, REDISMODULE_READ);

		if(keysOnly)
		{
			arr_len = 1;
			RedisModule_ReplyWithArray(ctx, 1);
			RedisModule_ReplyWithStringBuffer(ctx, pk_data, pk_len);	
		}
		else
		{
			RedisModuleString *pk_string = RedisModule_CreateString(ctx, pk_data, pk_len);
			RedisModuleKey *pk_key = (RedisModuleKey *)RedisModule_OpenKey(ctx, pk_string, REDISMODULE_READ);
			if(RedisModule_KeyType(pk_key) != REDISMODULE_KEYTYPE_EMPTY)
			{
				size_t pk_row_len = 0;
				const char *pk_row_data = RedisModule_StringDMA(pk_key, &pk_row_len, REDISMODULE_READ);
		
			
				arr_len = 1;
				RedisModule_ReplyWithArray(ctx, 1);
				RedisModule_ReplyWithStringBuffer(ctx, pk_row_data, pk_row_len);
			}
			RedisModule_CloseKey(pk_key);
		}
	}
	else if(key_type == REDISMODULE_KEYTYPE_HASH)
	{

	}
	
	// cleanup
	if(col_val_arr)
		free(col_val_arr);

	RedisModule_CloseKey(redis_key);

	if(arr_len == 0)
		RedisModule_ReplyWithArray(ctx, 0);

	return REDISMODULE_OK;
}

//============================================================
int RejiCreate_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
	RedisModule_AutoMemory(ctx);

    if (argc != 2)
		return RedisModule_WrongArity(ctx);

    CHECK_SCHEMA_LOADED(ctx);

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
    else if(res == SCHEMA_INDEX_EXISTS)
    {
        RedisModule_ReplyWithError(ctx, "Index already exists");
    }

	return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
}

//============================================================
int RejiDrop_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
	RedisModule_AutoMemory(ctx);
    
    if (argc != 2)
		return RedisModule_WrongArity(ctx);

    CHECK_SCHEMA_LOADED(ctx);

	size_t len = 0;
	const char *tmp = RedisModule_StringPtrLen(argv[1], &len);
	char *name = strndup(tmp, len);

    name = reji_str_to_lower(name);
	RedisModule_Log(ctx, "notice", "REJI: drop index: %*.*s", len, len, name);
	int res = reji_index_drop(name);

	if(res == SCHEMA_OK)
	{
		RedisModuleString *index_key_string = (RedisModuleString *)RedisModule_CreateStringPrintf(ctx, REDIS_SCHEMA_KEY);
		RedisModuleKey *redis_key = (RedisModuleKey *)RedisModule_OpenKey(ctx, index_key_string, REDISMODULE_READ | REDISMODULE_WRITE);
        
		RedisModuleString *rname = (RedisModuleString *)RedisModule_CreateStringPrintf(ctx, name);
		
		RedisModule_HashSet(redis_key, REDISMODULE_HASH_NONE, rname, REDISMODULE_HASH_DELETE, NULL);
		RedisModule_CloseKey(redis_key);
        DeleteAllKeys(ctx, name);

		return RedisModule_ReplyWithSimpleString(ctx, "OK");
	}
	else if(res == SCHEMA_INDEX_NOT_EXISTS)
    {
        RedisModule_ReplyWithError(ctx, "Index not found");
    }
    
	return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
}

/* REJI.PUT <key> <json_object> -- adds a record to Redis store and indexes it overwriting the existing one */
//============================================================
int RejiPut_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
	return RejiPut(ctx, argv, argc, false);
}

/* REJI.PUTNX <key> <json_object> -- adds a record to Redis store and indexes it failing on the existing one */
//============================================================
int RejiPutNX_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
	return RejiPut(ctx, argv, argc, true);
}

/* REJI.GET index key value [key1 value1 ...] - returns stored objects*/
//============================================================
int RejiGet_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
	return RejiGetKeysAndValues(false, ctx, argv, argc);
}

/* REJI.KEYS index key value [key1 value1 ...] - returns keys of the objects*/
//============================================================
int RejiKeys_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
	return RejiGetKeysAndValues(true, ctx, argv, argc);
}

/* This function must be present on each Redis module. It is used in order to
 * register the commands into the Redis server. */
//============================================================
int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);

    if(RedisModule_Init(ctx, "reji", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;
	
    if(RedisModule_CreateCommand(ctx, "reji.put", RejiPut_RedisCommand, "write fast", 0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if(RedisModule_CreateCommand(ctx, "reji.putnx", RejiPutNX_RedisCommand, "write fast", 0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

	if(RedisModule_CreateCommand(ctx, "reji.create", RejiCreate_RedisCommand, "write", 0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

	if(RedisModule_CreateCommand(ctx, "reji.drop", RejiDrop_RedisCommand, "write", 0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

	if(RedisModule_CreateCommand(ctx, "reji.get", RejiGet_RedisCommand, "readonly", 0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

	if(RedisModule_CreateCommand(ctx, "reji.keys", RejiKeys_RedisCommand, "readonly", 0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    reji_schema_init();
	RedisModule_Log(ctx, "notice", "REJI loaded");
    return REDISMODULE_OK;
}

// extern "C"
}
