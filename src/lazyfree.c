#include "server.h"
#include "bio.h"
#include "atomicvar.h"
#include "cluster.h"

static size_t lazyfree_objects = 0;
pthread_mutex_t lazyfree_objects_mutex = PTHREAD_MUTEX_INITIALIZER;



size_t lazyfreeGetPendingObjectsCount(void){
    return lazyfree_objects;
};

size_t lazyfreeGetFreeEffort(robj *obj){
    if(obj->type == OBJ_LIST){
        quicklist *ql = obj->ptr;
        return ql->len;
    }else if(obj->type == OBJ_SET && obj->encoding == OBJ_ENCODING_HT){
        dict *ht = obj->ptr;
        return dictSize(ht);
    }else if(obj->type == OBJ_ZSET && obj->encoding == OBJ_ENCODING_SKIPLIST){
        zset *zs = obj->ptr;
        return zs->zsl->length;
    }else if(obj->type == OBJ_HASH && obj->encoding == OBJ_ENCODING_HT){
        dict *ht = obj->ptr;
        return dictSize(ht);
    }else{
        return 1;
    }
};


#define LAZYFREE_THRESHOLD 64
int dbAsyncDelete(redisDb *db, robj *key){
    if(dictSize(db->expires) > 0) dictDelete(db->expires, key->ptr);

    dictEntry *de = dictUnlink(db->dict,key->ptr);
    if(de){
        robj *val = dictGetVal(de);
        size_t free_effort = lazyfreeGetFreeEffort(val);

        if(free_effort > LAZYFREE_THRESHOLD){
            atomicIncr(lazyfree_objects, 1, lazyfree_objects_mutex);
            bioCreateBackgroundJob(BIO_LAZY_FREE,val,NULL,NULL);
            dictSetVal(db->dict,de,NULL);
        };
    }

    if(de){
        dictFreeUnlinkedEntry(db->dict,de);
        if(server.cluster_enabled) slotToKeyDel(key);
        return 1;
    }else{
        return 0;
    }
};

void emptyDbAsync(redisDb *db){
    dict *oldht1 = db->dict, *oldht2 = db->expires;
    db->dict = dictCreate(&dbDictType,NULL);
    db->expires = dictCreate(&keyptrDictType,NULL);
    atomicIncr(lazyfree_objects, dictSize(oldht1),lazyfree_objects_mutex);
    bioCreateBackgroundJob(BIO_LAZY_FREE,NULL,oldht1,oldht2);
};


void slotToKeyFlushAsync(void){
    zskiplist *oldsl = server.cluster->slots_to_keys;
    server.cluster->slots_to_keys = zslCreate();
    atomicIncr(lazyfree_objects,oldsl->length,lazyfree_objects_mutex);
    bioCreateBackgroundJob(BIO_LAZY_FREE,NULL,NULL,oldsl);
};

void lazyfreeFreeObjectFromBioThread(robj *o){
    decrRefCount(o);
    atomicDecr(lazyfree_objects, 1, lazyfree_objects_mutex);
};

void lazyfreeFreeDatabaseFromBioThread(dict *ht1, dict *ht2){
    size_t numkeys = dictSize(ht1);
    dictRelease(ht1);
    dictRelease(ht2);
    atomicDecr(lazyfree_objects,numkeys,lazyfree_objects_mutex);
};

void lazyfreeFreeSlotsMapFromBioThread(zskiplist *sl){
    size_t len = sl->length;
    zslFree(sl);
    atomicDecr(lazyfree_objects,len,lazyfree_objects_mutex);
};















