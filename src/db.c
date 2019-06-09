#include "server.h"
#include "cluster.h"
#include "atomicvar.h"


#include <signal.h>
#include <ctype.h>

robj *lookupKey(redisDb *db, robj *key, int flags){
    dictEntry *de = dictFind(db->dict, key->ptr);
    if(de){
        robj *val = dictGetVal(de);

        if(server.rdb_child_pid == -1 && server.aof_child_pid && !(flags & LOOKUP_NOTOUCH)){
            if(server.maxmemory_policy & MAXMEMORY_FLAG_LFU){
                unsigned long ldt = val->lru >> 8;
                unsigned long counter = LFULogIncr(val->lru & 255);
                val->lru = (ldt << 8) | counter;
            }else{
                val->lru = LRU_CLOCK();
            }
        };
        return val;
    }else{
        return NULL;
    };
};

robj *lookupKeyReadWithFlags(redisDb *db, robj *key, int flags){
    robj *val;

    if(expireIfNeeded(db,key) == 1){
        if(server.masterhost == NULL) return NULL;

        if(server.current_client && server.current_client != server.master && server.current_client->cmd && (server.current_client->cmd->flags & CMD_READONLY)){
            return NULL;
        };
    };

    val = lookupKey(db,key,flags);
    if(val == NULL){
        server.stat_keyspace_misses++;
    }else{
        server.stat_keyspace_hits++;
    }
    return val;
};

robj *lookupKeyRead(redisDb *db, robj *key){
    return lookupKeyReadWithFlags(db,key,LOOKUP_NONE);
};

robj *lookupKeyWrite(redisDb *db, robj *key){
    expireIfNeeded(db,key);
    return lookupKey(db,key,LOOKUP_NONE);
};


robj *lookupKeyReadOrReply(client *c, robj *key, robj *reply){
    robj *o = lookupKeyRead(c->db,key);
    if(!o) addReply(c,reply);
    return o;
};

robj *lookupKeyWriteOrReply(client *c, robj *key, robj *reply){
    robj *o = lookupKeyWrite(c->db,key); 
    if(!o) addReply(c,reply);
    return o;
};

void dbAdd(redisDb *db, robj *key, robj *val){
   sds copy = sdsdup(key->ptr); 
   int retval = dictAdd(db->dict, copy,val);

   serverAssertWithInfo(NULL,key,retval == DICT_OK);
   if(val->type == OBJ_LIST) signalListAsReady(db,key);
   if(server.cluster_enabled) slotToKeyAdd(key);
};

void dbOverwrite(redisDb *db, robj *key, robj *val){
    dictEntry *de = dictFind(db->dict,key->ptr);
    serverAssertWithInfo(NULL,key,de != NULL);
    if(server.maxmemory_policy & MAXMEMORY_FLAG_LFU){
        robj *old = dictGetVal(de);
        int saved_lru = old->lru;
        dictReplace(db->dict,key->ptr,val);
        val->lru = saved_lru;
    }else{
        dictReplace(db->dict,key->ptr,val);
    }
};

void setKey(redisDb *db, robj *key, robj *val){
    if(lookupKeyWrite(db,key) == NULL){
        dbAdd(db,key,val);
    }else{
        dbOverwrite(db,key,val);
    };

    incrRefCount(val);
    removeExpire(db,key);
    signalModifiedKey(db,key);
};

int dbExists(redisDb *db, robj *key){
    return dictFind(db->dict, key->ptr) != NULL;
};

robj *dbRandomKey(redisDb *db){
    dictEntry *de;
    while(1){
        sds key;
        robj *keyobj;

        de = dictGetRandomKey(db->dict);
        if(de == NULL) return NULL;

        key = dictGetKey(de);

        keyobj = createStringObject(key,sdslen(key));
        if(dictFind(db->expires,key)){
            if(expireIfNeeded(db,keyobj)){
                decrRefCount(keyobj);
                continue;
            };
        };
        return keyobj;
    };
};

int dbSyncDelete(redisDb *db, robj *key){
    if(dictSize(db->expires) > 0) dictDelete(db->expires,key->ptr);
    if(dictDelete(db->dict, key->ptr) == DICT_OK){
        if(server.cluster_enabled) slotToKeyDel(key);
        return 1;
    }else{
        return 0;
    }
};

int dbDelete(redisDb *db, robj *key){
    return server.lazyfree_lazy_server_del ? dbAsyncDelete(db,key) : dbSyncDelete(db,key);
};

robj *dbUnshareStringValue(redisDb *db, robj *key, robj *o){
    serverAssert(o->type == OBJ_STRING);
    if(o->refcount != 1 || o->encoding != OBJ_ENCODING_RAW){
        robj *decoded = getDecodedObject(o);
        o = createRawStringObject(decoded->ptr,sdslen(decoded->ptr));
        decrRefCount(decoded);
        dbOverwrite(db,key,o);
    };
    return o;
};

long long emptyDb(int dbnum, int flags, void(callback)(void *)){
    int j , async = (flags & EMPTYDB_ASYNC);

    long long removed = 0;

    if(dbnum < -1 || dbnum >= server.dbnum){
        errno = EINVAL;
        return -1;
    };

    for(j = 0; j < server.dbnum; j++){
        if(dbnum != -1 || dbnum >= server.dbnum){
            removed += dictSize(server.db[j].dict);
        }

        if(async){
            emptyDbAsync(&server.db[j]);
        }else{
            dictEmpty(server.db[j].dict, callback);
            dictEmpty(server.db[j].expires, callback);
        }
    }

    if(server.cluster_enabled){
        if(async){
            slotToKeyFlushAsync();
        }else{
            slotToKeyFlush();
        }
    };

    if(dbnum == -1) flushSlaveKeysWithExpireList();
    return removed;
};


int selectDb(client *c, int id){
    if(id < 0 || id >= server.dbnum){
        return C_ERR;
    }

    c->db = &server.db[id];
    return C_OK;
};


void signalModifiedKey(redisDb *db, robj *key){
    touchWatchedKey(db,key);
};

void signalFlushedDb(int dbid){
    touchWatchedKeysOnFlush(dbid);
};

int getFlushCommandFlags(client *c, int *flags){
    if(c->argc > 1){
        if(c->argc > 2 || strcasecmp(c->argv[1]->ptr,"async")){
            addReply(c,shared.syntaxerr);
            return C_ERR;
        };
        *flags = EMPTYDB_ASYNC;
    }else{
        *flags = EMPTYDB_NO_FLAGS;
    }
    return C_OK;
};

void flushdbCommand(client *c){
    int flags;
    if(getFlushCommandFlags(c,&flags) == C_ERR) return;
    signalFlushedDb(c->db->id);
    server.dirty += emtpyDb(c->db->id,flags,NULL);
    addReply(c,shared.ok);
};

void flushallCommand(client *c){
    int flags;
    if(getFlushCommandFlags(c,&flags) == C_ERR)return;
    signalFlushedDb(-1);
    server.dirty += emptyDb(-1,flags,NULL);
    addReply(c,shared.ok);

    if(server.rdb_child_pid != -1){
        kill(server.rdb_child_pid,SIGUSR1);
        rdbRemoveTempFile(server.rdb_child_pid);
    };

    if(server.saveparamslen > 0){
        int saved_dirty = server.dirty;
        rdbSave(server.rdb_filename,NULL);
        server.dirty = saved_dirty;
    };

    server.dirty++;
};

void delGenericCommand(client *c, int lazy){
    int numdel = 0, j;
    for(j = 1; j < c->argc; j++){
        expireIfNeeded(c->db,c->argv[j]);
        int deleted = lazy ? dbAsyncDelete(c->db,c->argv[j]) : dbSyncDelete(c->db,c->argv[j]);

        if(deleted){
            signalModifiedKey(c->db,c->argv[j]);
            notifyKeyspaceEvent(NOTIFY_GENERIC,"del",c->argv[j],c->db->id);
            server.dirty++;
            numdel++;
        };
    };

    addReplyLongLong(c,numdel);
};

void deleteCommand(client *c){
    delGenericCommand(c,0);
};

void unlinkCommand(client *c){
    delGenericCommand(c,1);
};

void existsCommand(client *c){
    long long count = 0;
    int j;
    for(int j = 1; j < c->argc;j++){
        expireIfNeeded(c->db,c->argv[j]);
        if(dbExists(c->db,c->argv[j])) count++;
    };

    addReplyLongLong(c,count);
};

void selectCommand(client *c){
    long id;

    if(getLongFromObjectOrReply(c,c->argv[1],&id,"invalid DB index") != C_OK){
        return;
    };

    if(server.cluster_enabled && id != 0){
        addReplyError(c,"SELECT is not allowed in cluster mode");
        return;
    };

    if(selectDb(c,id) == C_ERR){
        addReplyError(c,"DB index is out of range");
    }else{
        addReply(c,shared.ok);
    }
};



void randomkeyCommand(client *c){
    robj *key;
    if((key=dbRandomKey(c->db)) == NULL){
        addReply(c,shared.nullbulk);
        return;
    };
    addReplyBulk(c,key);
    decrRefCount(key);
};


void keysCommand(client *c){
    dictIterator *di;
    dictEntry *de;

    sds pattern = c->argv[1]->ptr;
    int plen = sdslen(pattern), allkeys;
    unsigned long numkeys = 0;
    void *replylen = addDeferredMultiBulkLength(c);



};



void propagateExpire(redisDb *db, robj *key, int lazy){
    robj *argv[2];

    argv[0] = lazy ? shared.unlink : shared.del;

    argv[1] = key;
    incrRefCount(argv[0]);
    incrRefCount(argv[1]);

    if(server.aof_state != AOF_OFF){
        feedAppendOnlyFile(server.delCommand, db->id,argv,2);
    };

    replicationFeedSlaves(server.slaves, db->id,argv,2);

    decrRefCount(argv[0]);
    decrRefCount(argv[1]);
};

int expireIfNeeded(redisDb *db, robj *key){
    mstime_t when = getExpire(db,key);
    mstime_t now;

    if(when < 0) return 0;

    if(server.loading) return 0;

    now = server.lua_caller ? server.lua_time_start : mstime();

    if(server.masterhost != NULL) return now > when;

    if(now <= when) return 0;

    propagateExpire(db,key,server.lazyfree_lazy_expire);
    notifyKeyspaceEvent(NOTIFY_EXPIRED,"expired",key,db->id);
    return server.lazyfree_lazy_expire ? dbAsyncDelete(db,key) : dbSyncDelete(db,key);
};



long long getExpire(redisDb *db, robj *key){
    dictEntry *de;
    if(dictSize(db->expires) == 0 || (de = dictFind(db->expires,key->ptr)) == NULL){
        return -1;
    };

    serverAsertWithInfo(NULL,key,dictFind(db->dict,key->ptr) != NULL);
    return dictGetSignedIntegerVal(de);
};

int removeExpire(redisDb *db, robj *key){
    serverAssertWithInfo(NULL,key,dictFind(db->dict,key->ptr) != NULL);
    return dictDelete(db->expires,key->ptr) == DICT_OK;
};


void slotToKeyFlush(void){
    zslFree(server.cluster->slots_to_keys);
    server.cluster->slots_to_keys = zslCreate();
};














