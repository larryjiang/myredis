#include "server.h"
#include "cluster.h"
#include <dlfcn.h>

#define REDISMODULE_CORE = 1
#include "redismodule.h"

struct RedisModule{
    void *handle;
    char *name;
    int ver;
    int apiver;
    list *types;
};

typedef struct RedisModule RedisModule;
static dict *modules;

struct AutoMemEntry{
    void *ptr;
    int type;
};

#define REDISMODULE_AM_KEY 0
#define REDISMODULE_AM_STRING 1
#define REDISMODULE_AM_REPLY 2
#define REDISMODULE_AM_FREED 3 

#define REDISMODULE_POOL_ALLOC_MIN_SIZE (1024*8)
#define REDISMODULE_POOL_ALLOC_ALIGN (sizeof(void*))

typedef struct RedisModulePoolAllocBlock{
    uint32_t size;
    uint32_t used;

    struct RedisModulePoolAllocBlock *next;
    char memory[];
} RedisModulePoolAllocBlock;

struct RedisModuleCtx{
    void *getapifuncptr;
    struct RedisModule *module;
    client *client;
    struct AutoMemEntry *amqueue;
    int amqueue_len;
    int amqueue_used;
    int flags;
    void **postponed_arrays;
    int postponed_arrays_count;
    void *blocked_privdata;

    int *keys_pos;
    int keys_count;

    struct RedisModulePoolAllocBlock *pa_head;
};

typedef struct RedsiModuleCtx RedisModuleCtx;

#define REDISMODULE_CTX_INIT {(void*)(unsigned long)&RM_GetApi, NULL, NULL, NULL, 0, 0, 0, NULL, 0, NULL, NULL, 0, NULL}
#define REDISMODULE_CTX_MULTI_EMITTED (1<<0)
#define REDISMODULE_CTX_AUTO_MEMORY (1<<1)
#define REDISMODULE_CTX_KEYS_POS_REQUEST (1<<2)
#define REDISMODULE_CTX_BLOCKED_REPLY (1<<3)
#define REDISMODULE_CTX_BLOCKED_TIMEOUT (1<<4

struct RedisModuleKey{
    RedisModuleCtx *ctx;
    redisDb *db;
    robj *key;
    robj *value;
    void *iter;
    int mode;

    uint32_t ztype;
    zrangespec zrs;
    zlexrangespec zlrs;
    uint32_t zstart;
    uint32_t zend;
    void *zcurrent;
    int zer;
};


typedef struct RedisModuleKey RedisModuleKey;

#define REDISMODULE_ZSET_RANGE_NONE 0       /* This must always be 0. */
#define REDISMODULE_ZSET_RANGE_LEX 1
#define REDISMODULE_ZSET_RANGE_SCORE 2
#define REDISMODULE_ZSET_RANGE_POS 3

typedef int (*RedisModuleCmdFunc) (RedisModuleCtx *ctx, void **argv, int argc);

struct RedisModuleCommandProxy{
    struct RedisModule *module;
    RedisModuleCmdFunc func;
    struct redisCommand *rediscmd;
};

typedef struct RedisModuleCommandPoxy RedisModuleCommandProxy;

#define REDISMODULE_REPLYFLAG_NONE 0
#define REDISMODULE_REPLYFLAG_TOPARSE (1<<0) /* Protocol must be parsed. */
#define REDISMODULE_REPLYFLAG_NESTED (1<<1)  /* Nested reply object. No proto
                                                or struct free. */
typedef struct RedisModuleCallReply{
    RedisModuleCtx *ctx;
    int type;
    int flags;
    size_t len;
    char *proto;
    size_t protolen;

    union{
        const char *str;
        long long ll;
        struct RedisModuleCallReply *array;
    } val;
} RedisModuleCallReply;

typedef struct RedisModuleBlockedClient{
    client *client;

    RedisModule *module;
    RedisModuleCmdFunc reply_callback;
    RedisModuleCmdFunc timeout_callback;

    void (*free_privdata)(void*);
    void *privdata;
} RedisModuleBlockedClient;

static pthread_mutex_t moduleUnblockedClientsMutex = PTHREAD_MUTEX_INITIALIZER;
static list *moduleUnblockedClients;

void unblockClientFromModule(client *c){
    RedisModuleBlockedClient *bc = c->bpop.module_blocked_handle;
    bc->client = NULL;
};


