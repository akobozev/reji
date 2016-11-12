
#include "reji_schema.h"
#include <map>
#include "json.h"

typedef std::map<char*, reji_index_t*> IndexMap;

static IndexMap *g_index_map = NULL;

#define INDEX_NAME 		"name"
#define INDEX_COLUMNS 	"columns"
#define INDEX_UNIQUE	"unique"

//============================================================
// forward declaration
void reji_index_release(reji_index_t *index);

//============================================================
void reji_schema_init(char *indexes)
{
	g_index_map = new IndexMap();

	if(indexes)
	{
		// parse json index list
	}
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
	int res = 0;
	reji_index_t *index = NULL;

	do
	{
		
		// try to parse the record first
		struct json_tokener* tok = json_tokener_new();
		struct json_object *jobj = json_tokener_parse_ex(tok, json_data, json_data_len);

		if(jobj)
		{
			index = (reji_index_t*)malloc(sizeof(reji_index_t));

			json_object_object_foreach(jobj, key, val)
			{
				if(strcmp(key, INDEX_NAME) == 0)
				{
					index->name = strdup(json_object_get_string(val));
				}
				else if(strcmp(key, INDEX_UNIQUE) == 0)
				{
					if(json_object_get_boolean(val) > 0)
						index->flags |= REJI_INDEX_UNIQUE;
				}
				else if(strcmp(key, INDEX_COLUMNS) == 0 && json_object_is_type(val, json_type_array))
				{
					int arrLen = json_object_array_length(val);

					index->columns = (char **)calloc(arrLen, sizeof(char*));
					for(int i = 0; i < arrLen; i++)
					{
						struct json_object *arrVal = json_object_array_get_idx(val, i);
						index->columns[i] = strdup(json_object_get_string(arrVal));
					}
				}	
			}
			
			g_index_map->insert(std::pair<char*, reji_index_t*>(index->name, index));

			if(outIndex)
				*outIndex = index;
		}
	}
	while(0);

	if(res && index)
	{
		reji_index_release(index);
	}
	
	return res;
}

//============================================================
int reji_index_drop(char *indexName)
{

	return 0;
}

//============================================================
void reji_index_get(char *indexName, reji_index_t *index)
{

}

//============================================================
void reji_index_start(reji_index_iter_t *iter)
{

}

//============================================================
void reji_index_stop(reji_index_iter_t *iter)
{

}

//============================================================
void reji_index_next(reji_index_iter_t *iter)
{

}
