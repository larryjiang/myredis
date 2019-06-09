#include "server.h"
#include "bio.h"

#define EVPOOL_SIZE 16
#define EVPOOL_CACHED_SDS_SIZE 255

struct evictionPoolEntry {
    unsigned long long idle;
    sds key;
    sds cached;
    int dbid;
};


static struct evictionPoolEntry *evictionPoolLRU;

unsigned long LFUDecrAndReturn(robj *o);

unsigned int getLRUClock(void){
    return (mstime()/LRU_CLOCK_RESOLUTION) & LRU_CLOCK_MAX;
}


unsigned long long estimateObjectIdleTime(robj *o){
    unsigned long long lruclock = LRU_CLOCK();
    if(lruclock >= o->lru){
        return (lruclock - o->lru) * LRU_CLOCK_RESOLUTION;
    }else{
        return (lruclock + (LRU_CLOCK_MAX - o->lru)) * LRU_CLOCK_RESOLUTION;
    }
}

void evictionPoolAlloc(void){
    struct evictionPoolEntry *ep;
    int j;

    ep = zmalloc(sizeof(*ep) * EVPOOL_SIZE);
    for(j = 0; j < EVPOOL_SIZE; j++){
        ep[j].idle = 0;
        ep[j].key = NULL;
        ep[j].cached = sdsnewlen(NULL,EVPOOL_CACHED_SDS_SIZE);
        ep[j].dbid = 0;
    };

    evictionPoolLRU = ep;
};


void evictionPoolPopulate(int dbid, dict *sampledict, dict *keydict, struct evictionPoolEntry *pool){
    int j, k, count;
    dictEntry *samples[server.maxmemory_samples];

    count = dictGetSomeKeys(sampledict, samples, server.maxmemory_samples);
    for(j = 0; j < count; j++){
        unsigned long long idle;
        sds key;
        robj *o;
        dictEntry *de;

        de = samples[j];
        key = dictGetKey(de);

        if(server.maxmemory_policy != MAXMEMORY_VOLATILE_TTL){
            if(sampledict != keydict) de = dictFind(keydict, key);
            o = dictGetVal(de);
        }; 

        if(server.maxmemory_policy & MAXMEMORY_FLAG_LRU){
            idle = estimateObjectIdleTime(o);
        }else if(server.maxmemory_policy & MAXMEMORY_FLAG_LFU){
            idle = 255 - LFUDecrAndReturn(o); 
        }else if(server.maxmemory_policy  == MAXMEMORY_VOLATILE_TTL){
            idle = ULLONG_MAX - (long) dictGetVal(de);
        }else{
            serverPanic("Unknown eviction policy in evictionPoolPolulate()");
        };

        k = 0;
        while(k < EVPOOL_SIZE && pool[k].key && pool[k].idle < idle) k++;

        if(k == 0 && pool[EVPOOL_SIZE - 1].key != NULL){
            continue;
        }else if(k < EVPOOL_SIZE && pool[k].key == NULL){

        }else{
            if(pool[EVPOOL_SIZE-1].key == NULL){
               sds cached = pool[EVPOOL_SIZE - 1].cached;
               memmove(pool+k+1,pool+k,sizeof(pool[0]) * (EVPOOL_SIZE -k - 1)); 
               pool[k].cached = cached;
            }else{
               k--;
               sds cached = pool[0].cached;
               if(pool[0].key != pool[0].cached) sdsfree(pool[0].key);
               memmove(pool,pool+1,sizeof(pool[0]) * k);
               pool[k].cached = cached;
            }
        }

        int klen = sdslen(key);
        if(klen > EVPOOL_CACHED_SDS_SIZE){
            pool[k].key = sdsdup(key);
        }else{
            memcpy(pool[k].cached, key, klen+1);
            sdssetlen(pool[k].cached, klen);
            pool[k].key = pool[k].cached;
        }

        pool[k].idle = idle;
        pool[k].dbid = dbid;
    };
};


unsigned long LFUGetTimeInMinutes(void){
    return (server.unixtime/60) & 65535;
};


unsigned long LFUTimeElapsed(unsigned long ldt){
    unsigned long now = LFUGetTimeInMinutes();
    if(now >= ldt) return now - ldt;
    return 65535 - ldt + now;
};

uint8_t LFULogIncr(uint8_t counter){
    if(counter == 255) return 255;
    double r = (double) rand()/RAND_MAX;
    double baseval = counter - LFU_INIT_VAL;
    if(baseval < 0) baseval = 0;

    double p = 1.0 / (baseval * server.lfu_log_factor + 1);
    if(r < p) counter++;
    return counter;
};




#define LFU_DECR_INTERVAL 1
unsigned long LFUDecrAndReturn(robj *o){
    unsigned long ldt = o->lru >> 8;
    unsigned long counter = o->lru & 255;
    if(LFUTimeElapsed(ldt) >= server.lfu_decay_time && counter){
        if(counter > LFU_INIT_VAL * 2){
            counter /= 2;
            if(counter < LFU_INIT_VAL * 2) counter = LFU_INIT_VAL * 2;
        }else{
            counter--;
        }
        o->lru = (LFUGetTimeInMinutes()<<8) | counter;
    };
    return counter;
};



size_t freeMemoryGetNotCountedMemory(void){
    size_t overhead = 0;
    int slaves = listLength(server.slaves);


    if(slaves){
        listIter li;
        listNode *ln;

        listRewind(server.slaves,&li);
        while((ln=listNext(&li))){
            client *slave = listNodeValue(ln);
            overhead += getClientOutputBufferMemoryUsage(slave);
        };
    };

    if(server.aof_state != AOF_OFF){
        overhead += sdslen(server.aof_buf) + aofRewriteBufferSize();
    };

    return overhead;
};


int freeMemoryIfNeeded(void){
    size_t mem_reported, mem_used, mem_tofree, mem_freed;
    mstime_t latency, eviction_latency;
    long long delta;

    int slaves = listLength(server.slaves); 
    mem_reported = zmalloc_used_memory(); 

    if(mem_reported <= server.maxmemory) return C_OK;
    mem_used = mem_reported;

    size_t overhead = freeMemoryGetNotCountedMemory();
    mem_used = (mem_used > overhead) ? mem_used - overhead : 0;
    if(mem_used <= server.maxmemory) return C_OK;


    mem_tofree = mem_used - server.maxmemory;
    mem_freed = 0;

    if(server.maxmemory_policy == MAXMEMORY_NO_EVICTION){
        goto cant_free;
    };


    latencyStartMonitor(latency);
    while(mem_freed < mem_tofree){
        int j, k, i, keys_freed = 0;
        static int next_db = 0;
        sds bestKey = NULL;
        int bestdbid;
        redisDb *db;
        dict *dict;
        dictEntry *de;

        if(server.maxmemory_policy & (MAXMEMORY_FLAG_LRU | MAXMEMORY_FLAG_LFU)|| 
        server.maxmemory_policy == MAXMEMORY_VOLATILE_TTL){
            struct evictionPoolEntry *pool = evictionPoolLRU;
            while(bestKey == NULL){
                unsigned long total_keys = 0, keys;

                for(i = 0; i < server.dbnum; i++){
                    db = server.db + i;
                    dict = (server.maxmemory_policy & MAXMEMORY_FLAG_ALLKEYS) ? db->dict : db->expires;
                    if((keys = dictSize(dict)) != 0){
                        evictionPoolPopulate(i,dict, db->dict, pool);
                        total_keys += keys;
                    };
                };
                if(!total_keys) break;
                for(k = EVPOOL_SIZE - 1; k >= 0; k--){
                    if(pool[k].key == NULL) continue;
                    bestdbid = pool[k].dbid;

                    if(server.maxmemory_policy & MAXMEMORY_FLAG_ALLKEYS){
                        de = dictFind(server.db[pool[k].dbid].dict,pool[k].key);
                    }else{
                        de = dictFind(server.db[pool[k].dbid].expires, pool[k].key);
                    };

                    if(pool[k].key != pool[k].cached){
                        sdsfree(pool[k].key);
                    }

                    pool[k].key = NULL;
                    pool[k].idle = 0;

                    if(de){
                        bestKey = dictGetKey(de);
                        break;
                    }else{

                    };
                };
            };

        }else if(server.maxmemory_policy == MAXMEMORY_ALLKEYS_RANDOM ||
                 server.maxmemory_policy == MAXMEMORY_VOLATILE_RANDOM
        ){
            for(i = 0; i < server.dbnum; i++){
                j = (++next_db) % server.dbnum;
                db = server.db++;
                dict = (server.maxmemory_policy == MAXMEMORY_ALLKEYS_RANDOM) ? db->dict : db->expires;
                if(dictSize(dict)!= 0){
                    de = dictGetRandomKey(dict);
                    bestKey = dictGetKey(de);
                    bestdbid = j;
                    break;
                };
            };
        };

        if(bestKey){
            db = server.db + bestdbid;
            robj * keyobj = createStringObject(bestKey, sdslen(bestKey));
            propagateExpire(db,keyobj,server.lazyfree_lazy_eviction);

            delta = (long long)zmalloc_used_memory();
            latencyStartMonitor(eviction_latency);
            if(server.lazyfree_lazy_eviction){
                dbAsyncDelete(db,keyobj);
            }else{
                dbSyncDelete(db,keyobj);
            }
            latencyEndMonitor(eviction_latency);
            latencyAddSampleIfNeeded("eviction-del",eviction_latency);
            latencyRemoveNestedEvent(latency,eviction_latency);
            delta -= (long long) zmalloc_used_memory();
            mem_freed += delta;

            server.stat_evictedkeys++;
            notifyKeyspaceEvent(NOTIFY_EVICTED,"evicted",keyobj,db->id);
            decrRefCount(keyobj);
            keys_freed++;

            if(slaves) flushSlavesOutputBuffers();
            
            if(server.lazyfree_lazy_eviction && !(keys_freed % 16)){
                overhead = freeMemoryGetNotCountedMemory();
                mem_used = zmalloc_used_memory();
                mem_used = (mem_used > overhead) ? mem_used - overhead : 0;
                if(mem_used <= server.maxmemory){
                    mem_freed = mem_tofree;
                }
            };
        }

        if(!keys_freed){
            latencyEndMonitor(latency);
            latencyAddSampleIfNeeded("eviction-cycle",latency);
            goto cant_free;
        }
    };

    latencyEndMonitor(latency);
    latencyAddSampleIfNeeded("eviction-cycle",latency);
    return C_OK;

    cant_free:

        while(bioPendingJobsOfType(BIO_LAZY_FREE)){
            if(((mem_reported - zmalloc_used_memory()) + mem_freed) >= mem_tofree){
                break;
            };
            usleep(1000);
        };
        return C_ERR;
}

