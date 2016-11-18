
#include "reji_schema.h"

typedef std::unordered_map<std::string, reji_index_t *> IndexMap;
typedef std::pair<std::string, reji_index_t *> IndexPair;
typedef std::unordered_map<std::string, const char *> JsonObjectMap;

static IndexMap *g_index_map = NULL;

#define INDEX_NAME 		"name"
#define INDEX_COLUMNS 	"columns"
#define INDEX_UNIQUE	"unique"

// forward declarations
//============================================================
void reji_index_release(reji_index_t *index);
bool reji_build_index_key(reji_index_t *index, JsonObjectMap& obj_map, reji_index_key_t &index_key);
char *str_to_lower(char *);

//============================================================
void reji_schema_init()
{
	g_index_map = new IndexMap();
}

//============================================================
void reji_schema_fini()
{
	if(g_index_map)
	{
		for(IndexMap::iterator it = g_index_map->begin(); it != g_index_map->end(); it++)
		{
			if(it->second)
			{
				delete it->second;
			}
		}

		g_index_map->clear();

		delete g_index_map;
		g_index_map = NULL;
	}
}

//============================================================
int reji_index_create(const char *json_data, size_t json_data_len, reji_index_t **outIndex)
{
	int res = SCHEMA_FAIL;
	reji_index_t *index = NULL;

    struct json_tokener* tok = json_tokener_new();
    json_tokener_set_flags(tok, 0);

	do
	{
		// try to parse the record first
		struct json_object *jobj = json_tokener_parse_ex(tok, json_data, json_data_len);

		//printf("index start parsing\n");
		if(jobj)
		{
			json_object *val = NULL;

            if(!json_object_object_get_ex(jobj, INDEX_NAME, &val))
			{
				break;
			}

            char *tmp = strdup(json_object_get_string(val));

            tmp = str_to_lower(tmp);
            IndexMap::iterator it = g_index_map->find(tmp);
            if(it != g_index_map->end())
            {
                res = SCHEMA_INDEX_EXISTS;
                free(tmp);
                break;
            }
            
			index = (reji_index_t*)calloc(1, sizeof(reji_index_t));
            index->name = tmp;
			//printf("index name: %s\n", index->name);
			
			if(json_object_object_get_ex(jobj, INDEX_UNIQUE, &val))
			{
				if(json_object_get_boolean(val) > 0)
					index->flags |= REJI_INDEX_UNIQUE;
			}
			
			if(!json_object_object_get_ex(jobj, INDEX_COLUMNS, &val) || 
			   !json_object_is_type(val, json_type_array))
			{
				break;
			}

			int arrLen = json_object_array_length(val);
			//printf("column num: %d\n", arrLen);
			index->columns = (char **)calloc(arrLen, sizeof(char*));
			index->numColumns = arrLen;
			
			for(int i = 0; i < arrLen; i++)		
			{		
				json_object *arrVal = json_object_array_get_idx(val, i);
				index->columns[i] = strdup(json_object_get_string(arrVal));
                index->columns[i] = str_to_lower(index->columns[i]);
				//printf("column[%d]: %s\n", i, index->columns[i]);
			}
			
			g_index_map->insert(IndexPair(index->name, index));

			if(outIndex)
				*outIndex = index;

			res = SCHEMA_OK;
		}
	}
	while(0);

	if(tok)
		json_tokener_free(tok);
	
	if(res && index)
		reji_index_release(index);
	
	return res;
}

//============================================================
int reji_index_drop(char *indexName)
{
    IndexMap::iterator it = g_index_map->find(indexName);

	if(it != g_index_map->end())
	{
		//printf("drop index: %s\n", it->second->name);
		reji_index_release(it->second);
		g_index_map->erase(it);
        return SCHEMA_OK;
	}
	
	return SCHEMA_INDEX_NOT_EXISTS;
}

//============================================================
int reji_index_get(const char *indexName, size_t len, reji_index_t **index)
{
    char *name = strndup(indexName, len);
    name = str_to_lower(name);

    IndexMap::iterator it = g_index_map->find(name);

    if(it != g_index_map->end() && index)
    {
        *index = it->second;
        return SCHEMA_OK;
    }

    return SCHEMA_FAIL;
}

//============================================================
int reji_index_start(reji_index_iter_t **iter)
{
    IndexMap::iterator it = g_index_map->begin();
    if(it != g_index_map->end() && iter)
    {
        *iter = (reji_index_iter_t*)malloc(sizeof(reji_index_iter_t));
        (*iter)->index = it->second;
        return SCHEMA_OK;
    }

    return SCHEMA_FAIL;
}

//============================================================
void reji_index_stop(reji_index_iter_t *iter)
{
    if(iter)
    {
        iter->index = NULL;
        free(iter);
    }
}

//============================================================
int reji_index_next(reji_index_iter_t *iter)
{
    int res = SCHEMA_FAIL;
    
    do
    {
        if(iter)
        {
            reji_index_t *index = iter->index;

            iter->index = NULL;
            if(index)
            {
                IndexMap::iterator it = g_index_map->find(iter->index->name);
                if(it != g_index_map->end())
                {
                    it++;
                    if(it != g_index_map->end())
                    {
                        iter->index = it->second;
                        res = SCHEMA_OK;
                    }
                }
            }
        }
    }
    while(0);

    return res;
}

//============================================================
int reji_build_index_keys(json_object *jobj, reji_index_keys_map_t& keys_map)
{
	int res = INDEX_OK;

	// index the JSON object fields first
	JsonObjectMap jobj_map; 

	json_object_object_foreach(jobj, jobj_key, jobj_val)
	{
		enum json_type jobj_type = json_object_get_type(jobj_val);
		
		// skip all the object and array fields
		if(jobj_type == json_type_array || jobj_type == json_type_object)
			continue;

		// associate field name with string value
		jobj_map[str_to_lower(jobj_key)] = json_object_get_string(jobj_val);
		
	}

	// iterate over the indices and build the index keys
    for(IndexMap::iterator idx_it = g_index_map->begin(); idx_it != g_index_map->end(); ++idx_it)
	{
		reji_index_key_t index_key = {NULL, NULL};

		// TODO: check return value (?)
		reji_build_index_key(idx_it->second, jobj_map, index_key);
		keys_map[index_key.key] = index_key;
	}
	
	return res;
}	

//============================================================
void reji_free_index_keys(reji_index_keys_map_t& keys_map)
{
	for(reji_index_keys_map_t::iterator it = keys_map.begin(); it != keys_map.end(); ++it)
		reji_free_index_key(it->second);
}

//============================================================
void reji_string_to_lower(std::string& src)
{
	for(size_t i = 0; i < src.length(); i++)
	{
		src[i] = tolower(src[i]);
	}
}

//============================================================
bool reji_build_key(reji_index_t *index, IndexValMap& obj_map, std::string& key)
{
	bool res = true;
	key += REDIS_INDEX_KEY_PREFIX;

	key += REDIS_INDEX_KEY_SEPARATOR;
	key += index->name;

	// iterate over index fields and build a key from correspondent jobj values
	for(int i = 0; i < index->numColumns; i++)
	{
		IndexValMap::iterator it = obj_map.find(index->columns[i]);

		key += REDIS_INDEX_KEY_SEPARATOR;
		
		if(it != obj_map.end())
			key.append(it->second->val, it->second->val_len);
		else
		{
			res = false;
			break;
		}
	}

	return res;
}	

// PRIVATE
//============================================================
void reji_index_release(reji_index_t *index)
{
	if(index->columns)
	{
		for(int i = 0; i < index->numColumns; i++)
		{
			if(index->columns[i])
				free(index->columns[i]);
		}

		free(index->columns);
	}
	
	if(index->name)
		free(index->name);

	free(index);
}

//============================================================
bool reji_build_index_key(reji_index_t *index, JsonObjectMap& obj_map, reji_index_key_t& index_key)
{
	bool res = true;
	std::string key_value(REDIS_INDEX_KEY_PREFIX);

	key_value += REDIS_INDEX_KEY_SEPARATOR;
	key_value += index->name;

	// iterate over index fields and build a key from correspondent jobj values
	for(int i = 0; i < index->numColumns; i++)
	{
		JsonObjectMap::iterator it = obj_map.find(index->columns[i]);

		key_value += REDIS_INDEX_KEY_SEPARATOR;
		
		if(it != obj_map.end())
			key_value += it->second;
		else
			res = false;
	}

	index_key.key = strdup(key_value.c_str());
	index_key.index = index;
	
	return res;
}	

//============================================================
void reji_free_index_key(reji_index_key_t& index_key)
{
	if(index_key.key)
		free((void *)index_key.key);
}	

//============================================================
char *reji_str_to_lower(char *src)
{
    return str_to_lower(src);
}

//============================================================
char *str_to_lower(char *str)
{
	char *res = str;
	for(; *str; ++str) *str = tolower(*str);
	return res;
}
