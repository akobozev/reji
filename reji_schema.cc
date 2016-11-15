
#include "reji_schema.h"
#include <map>
#include "json.h"

typedef std::map<char *, reji_index_t *, bool(*)(char *, char *)> IndexMap;
typedef std::pair<char *, reji_index_t *> IndexPair;

typedef std::map<char *, char *, bool(*)(char *,char *)> DataObjectMap;

static IndexMap *g_index_map = NULL;

#define INDEX_NAME 		"name"
#define INDEX_COLUMNS 	"columns"
#define INDEX_UNIQUE	"unique"

// forward declarations
//============================================================
void reji_index_release(reji_index_t *index);
bool stringCompareIgnoreCase(char *lhs, char *rhs);

//============================================================
void reji_schema_init()
{
	g_index_map = new IndexMap(stringCompareIgnoreCase);
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

            const char *tmp = json_object_get_string(val);

            IndexMap::iterator it = g_index_map->find((char*)tmp);
            if(it != g_index_map->end())
            {
                res = SCHEMA_INDEX_EXISTS;
                break;
            }
            
			index = (reji_index_t*)calloc(1, sizeof(reji_index_t));
            index->name = strdup(tmp);
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
					
			for(int i = 0; i < arrLen; i++)		
			{		
				json_object *arrVal = json_object_array_get_idx(val, i);
				index->columns[i] = strdup(json_object_get_string(arrVal));	
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
	{
		json_tokener_free(tok);
	}
	
	if(res && index)
	{
		reji_index_release(index);
	}
	
    json_tokener_free(tok);

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
int reji_index_get(char *indexName, reji_index_t **index)
{
    IndexMap::iterator it = g_index_map->find(indexName);

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
bool stringCompareIgnoreCase(char *lhs, char *rhs)
{
	return (strcasecmp(lhs, rhs) < 0);
}
