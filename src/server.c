#include "server.h"
#include "cluster.h"
#include "slowlog.h" 
#include "bio.h"
#include "latency.h"


#include <time.h>
#include <signal.h>
#include <sys/wait.h>
#include <errno.h>
#include <assert.h>
#include <ctype.h>
#include <stdarg.h>
#include <arpa/inet.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/uio.h> 
#include <sys/un.h> 
#include <limits.h>
#include <float.h>
#include <math.h>
#include <sys/utsname.h>
#include <locale.h>
#include <sys/socket.h>


struct sharedObjectsStruct shared;


double R_Zero, R_PosInf, R_NegInf, R_Nan;

struct redisServer server;


struct redisCommand redisCommandTable[] = {
    {"module", moduleCommand, -2, "as",0, NULL, 1,1,1,0,0},
    {"get",getCommand,2,"rF",0,NULL,1,1,1,0,0},
    {"set",setCommand,-3,"wm",0,NULL,1,1,1,0,0},
    {"setnx",setnxCommand,3,"wmF",0,NULL,1,1,1,0,0},
    {"setex",setexCommand,4,"wm",0,NULL,1,1,1,0,0},
    {"psetex",psetexCommand,4,"wm",0,NULL,1,1,1,0,0},
    {"append",appendCommand,3,"wm",0,NULL,1,1,1,0,0},
    {"strlen",strlenCommand,2,"rF",0,NULL,1,1,1,0,0},
    {"del",delCommand,-2,"w",0,NULL,1,-1,1,0,0},
    {"unlink",unlinkCommand,-2,"wF",0,NULL,1,-1,1,0,0},
    {"exists",existsCommand,-2,"rF",0,NULL,1,-1,1,0,0},
    {"setbit",setbitCommand,4,"wm",0,NULL,1,1,1,0,0},
    {"getbit",getbitCommand,3,"rF",0,NULL,1,1,1,0,0},
    {"bitfield",bitfieldCommand,-2,"wm",0,NULL,1,1,1,0,0},
    {"setrange",setrangeCommand,4,"wm",0,NULL,1,1,1,0,0},
    {"getrange",getrangeCommand,4,"r",0,NULL,1,1,1,0,0},
    {"substr",getrangeCommand,4,"r",0,NULL,1,1,1,0,0},
    {"incr",incrCommand,2,"wmF",0,NULL,1,1,1,0,0},
    {"decr",decrCommand,2,"wmF",0,NULL,1,1,1,0,0},
    {"mget",mgetCommand,-2,"r",0,NULL,1,1,1,0,0},
    {"rpush",rpushCommand,-3,"wmF",0,NULL,1,1,1,0,0},
    {"lpush",lpushCommand,-3,"wmF",0,NULL,1,1,1,0,0},
    {"lpushx",lpushxCommand,-3,"wmF",0,NULL,1,1,1,0,0},
    {"rpushx",rpushxCommand,-3,"wmF",0,NULL,1,1,1,0,0},
    {"linsert",linsertCommand,5,"wm",0,NULL,1,1,1,0,0},
    {"lpop",lpopCommand,2,"wF",0,NULL,1,1,1,0,0},
    {"rpop",rpopCommand,2,"wF",0,NULL,1,1,1,0,0},
    {"brpop",brpopCommand,-3,"ws",0,NULL,1,1,1,0,0},
    {"brpoplpush",brpoplpushCommand,4,"wms",0,NULL,1,2,1,0,0},
    {"blpop",blpopCommand,-3,"ws",0,NULL,1,-2,1,0,0},
    {"llen",llenCommand,2,"rF",0,NULL,1,1,1,0,0},
    {"lindex",lindexCommand,3,"r",0,NULL,1,1,1,0,0},
    {"lset",lsetCommand,4,"wm",0,NULL,1,1,1,0,0},
    {"lrange",lrangeCommand,4,"r",0,NULL,1,1,1,0,0},
    {"ltrim",ltrimCommand,4,"w",0,NULL,1,1,1,0,0},
    {"lrem",lremCommand,4,"w",0,NULL,1,1,1,0,0},
    {"rpoplpush",rpoplpushCommand,3,"wm",0,NULL,1,2,1,0,0},
    {"sadd",saddCommand,-3,"wmF",0,NULL,1,1,1,0,0},
    {"srem",sremCommand,-3,"wF",0,NULL,1,1,1,0,0},
    {"smove",smoveCommand,4,"wF",0,NULL,1,2,1,0,0},
    {"sismember",sismemberCommand,3,"rF",0,NULL,1,1,1,0,0},
    {"scard",scardCommand,2,"rF",0,NULL,1,1,1,0,0},
    {"spop",spopCommand,-2,"wRF",0,NULL,1,1,1,0,0},
    {"srandmember",srandmemberCommand,-2,"rR",0,NULL,1,1,1,0,0},
    {"sinter",sinterCommand,-2,"rS",0,NULL,1,-1,1,0,0},
    {"sinterstore",sinterstoreCommand,-3,"wm",0,NULL,1,-1,1,0,0}, 
    {"sunion",sunionCommand,-2,"rS",0,NULL,1,-1,1,0,0},
    {"sunionstore",sunionstoreCommand,-3,"wm",0,NULL,1,-1,1,0,0},
    {"sdiff",sdiffCommand,-2,"rS",0,NULL,1,-1,1,0,0},
    {"sdiffstore",sdiffstoreCommand, -3,"wm",0,NULL,1,-1,1,0,0},
    {"smembers",sinterCommand,2,"rS",0,NULL,1,1,1,0,0},
    {"sscan",sscanCommand,-3,"rR",0,NULL,1,1,1,0,0},
    {"zadd",zaddCommand,-4,"wmF",0,NULL,1,1,1,0,0},
    {"zincrby",zincrbyCommand,4,"wmF",0,NULL,1,1,1,0,0},
    {"zrem",zremCommand,-3,"wF",0,NULL,1,1,1,0,0},
    {"zremrangebyscore",zremrangebyscoreCommand,4,"w",0,NULL,1,1,1,0,0},
    {"zremrangebyrank",zremrangebyrankCommand,4,"w",0,NULL,1,1,1,0,0},
    {"zremrangebylex",zremrangebylexCommand,4,"w",0,NULL,1,1,1,0,0},
    {"zunionstore",zunionstoreCommand,-4,"wm",0,zunionInterGetKeys,0,0,0,0,0},
    {"zinterstore",zinterstoreCommand,-4,"wm",0,zunionInterGetKeys,0,0,0,0,0},
    {"zrange",zrangeCommand,-4,"r",0,NULL,1,1,1,0,0},
    {"zrangebyscore",zrangebyscoreCommand,-4,"r",0,NULL,1,1,1,0,0},
    {"zrevrangebyscore",zrevrangebyscoreCommand,-4,"r",NULL,1,1,1,0,0},
    {"zrangebylex",zrangebylexCommand,-4,"r",0,NULL,1,1,1,0,0},
    {"zrevrangebylex",zrevrangebylexCommand,-4,"r",0,NULL,1,1,1,0,0},
    {"zcount",zcountCommand,4,"rF",0,NULL,1,1,1,0,0},
    {"zlexcount",zlexcountCommand,4,"rF",0,NULL,1,1,1,0,0},
    {"zrevrange",zrevrangeCommand,-4,"r",0,NULL,1,1,1,0,0},
    {"zcard",zcardCommand,2,"rF",0,NULL,1,1,1,0,0},
    {"zscore",zscoreCommand,3,"rF",0,NULL,1,1,1,0,0},
    {"zrank",zrankCommand,3,"rF",0,NULL,1,1,1,0,0},
    {"zrevrank",zrevrankCommand,3,"rF",0,NULL,1,1,1,0,0},
    {"zscan",zscanCommand,-3,"rR",0,NULL,1,1,1,0,0},
    {"hset",hsetCommand,4,"wmF",0,NULL,1,1,1,0,0},
    {"hsetnx",hsetnxCommand,4,"wmF",0,NULL,1,1,1,0,0},        
    {"hget",hgetCommand,3,"rF",0,NULL,1,1,1,0,0},
    {"hmset",hmsetCommand,-4,"wm",0,NULL,1,1,1,0,0},
    {"hmget",hmgetCommand,-3,"r",0,NULL,1,1,1,0,0},
    {"hincrby",hincrbyCommand,4,"wmF",0,NULL,1,1,1,0,0},
    {"hincrbyfloat",hincrbyfloatCommand,4,"wmF",0,NULL,1,1,1,0,0},
    {"hdel",hdelCommand,-3,"wF",0,NULL,1,1,1,0,0},
    {"hlen",hlenCommand,2,"rF",0,NULL,1,1,1,0,0},
    {"hstrlen",hstrlenCommand,3,"rF",0,NULL,1,1,1,0,0},
    {"hkeys",hkeysCommand,2,"rS",0,NULL,1,1,1,0,0},
    {"hvals",hvalsCommand,2,"rS",0,NULL,1,1,1,0,0},
    {"hgetall",hgetallCommand,2,"r",0,NULL,1,1,1,0,0},
    {"hexists",hexistsCommand,3,"rF",0,NULL,1,1,1,0,0},
    {"hscan",hscanCommand,-3,"rR",0,NULL,1,1,1,0,0},
    {"incrby",incrbyCommand,3,"wmF",0,NULL,1,1,1,0,0},
    {"decrby",decrbyCommand,3,"wmF",0,NULL,1,1,1,0,0},
    {"incrbyfloat",incrbyfloatCommand,3,"wmF",0,NULL,1,1,1,0,0},
    {"getset",getsetCommand,3,"wm",0,NULL,1,1,1,0,0},
    {"mset",msetCommand,-3,"wm",0,NULL,1,-1,2,0,0},
    {"msetnx",msetnxCommand,-3,"wm",0,NULL,1,-1,2,0,0},
    {"randomkey",randomkeyCommand,1,"rR",0,NULL,0,0,0,0,0},
    {"select",selectCommand,2,"lF",0,NULL,0,0,0,0,0},
    {"swapdb",swapdbCommand,3,"wF",0,NULL,0,0,0,0,0},
    {"move",moveCommand,3,"wF",0,NULL,1,1,1,0,0},
    {"rename",renameCommand,3,"w",0,NULL,1,2,1,0,0},
    {"renamenx",renamenxCommand,3,"wF",0,NULL,1,2,1,0,0},
    {"expire",expireCommand,3,"wF",0,NULL,1,1,1,0,0},
    {"expireat",expireatCommand,3,"wF",0,NULL,1,1,1,0,0},
    {"keys",keysCommand,2,"rS",0,NULL,0,0,0,0,0},
    {"scan",scanCommand,-2,"rR",0,NULL,0,0,0,0,0},
    {"dbsize",dbsizeCommand,1,"rF",0,NULL,0,0,0,0,0},
    {"auth",authCommand,2,"sltF",0,NULL,0,0,0,0,0},
    {"ping",pingCommand,-1,"tF",0,NULL,0,0,0,0,0},
    {"echo",echoCommand,2,"F",0,NULL,0,0,0,0,0},
    {"save",saveCommand,1,"as",0,NULL,0,0,0,0,0},
    {"bgsave",bgsaveCommand,-1,"a",0,NULL,0,0,0,0,0},
    {"bgrewriteaof",bgrewriteaofCommand,1,"a",0,NULL,0,0,0,0,0},
    {"shutdown",shutdownCommand,-1,"alt",0,NULL,0,0,0,0,0},
    {"lastsave",lastsaveCommand,1,"RF",0,NULL,0,0,0,0,0},
    {"type",typeCommand,2,"rF",0,NULL,1,1,1,0,0},
    {"multi",multiCommand,1,"sF",0,NULL,0,0,0,0,0},
    {"exec",execCommand,1,"sM",0,NULL,0,0,0,0,0},
    {"discard",discardCommand,1,"sF",0,NULL,0,0,0,0,0},
    {"sync",syncCommand,1,"ars",0,NULL,0,0,0,0,0},
    {"psync",syncCommand,3,"ars",0,NULL,0,0,0,0,0},
    {"replconf",replconfCommand,-1,"aslt",0,NULL,0,0,0,0,0},
    {"flushdb",flushdbCommand,-1,"w",0,NULL,0,0,0,0,0},
    {"flushall",flushallCommand,-1,"w",0,NULL,0,0,0,0,0},
    {"sort",sortCommand,-2,"wm",0,sortGetKeys,1,1,1,0,0},
    {"info",infoCommand,-1,"lt",0,NULL,0,0,0,0,0},
    {"monitor",monitorCommand,1,"as",0,NULL,0,0,0,0,0},
    {"ttl",ttlCommand,2,"rF",0,NULL,1,1,1,0,0},
    {"touch",touchCommand,-2,"rF",0,NULL,1,1,1,0,0},
    {"pttl",pttlCommand,2,"rF",0,NULL,1,1,1,0,0},
    {"persist",persistCommand,2,"wF",0,NULL,1,1,1,0,0},
    {"slaveof",slaveofCommand,3,"ast",0,NULL,0,0,0,0,0},
    {"role",roleCommand,1,"lst",0,NULL,0,0,0,0,0},
    {"debug",debugCommand,-1,"as",0,NULL,0,0,0,0,0},
    {"config",configCommand,-2,"lat",0,NULL,0,0,0,0,0},
    {"subscribe",subscribeCommand,-2,"pslt",0,NULL,0,0,0,0,0},
    {"unsubscribe",unsubscribeCommand,-1,"pslt",0,NULL,0,0,0,0,0},
    {"psubscribe",psubscribeCommand,-2,"pslt",0,NULL,0,0,0,0,0},
    {"punsubscribe",punsubscribeCommand,-1,"pslt",0,NULL,0,0,0,0,0},
    {"publish",publishCommand,3,"pltF",0,NULL,0,0,0,0,0},
    {"pubsub",pubsubCommand,-2,"pltR",0,NULL,0,0,0,0,0},
    {"watch",watchCommand,-2,"sF",0,NULL,1,-1,1,0,0},
    {"unwatch",unwatchCommand,1,"sF",0,NULL,0,0,0,0,0},
    {"cluster",clusterCommand,-2,"a",0,NULL,0,0,0,0,0},
    {"restore",restoreCommand,-4,"wm",0,NULL,1,1,1,0,0},
    {"restore-asking",restoreCommand,-4,"wmk",0,NULL,1,1,1,0,0},
    {"migrate",migrateCommand,-6,"w",0,migrateGetKeys,0,0,0,0,0},
    {"asking",askingCommand,1,"F",0,NULL,0,0,0,0,0},
    {"readonly",readonlyCommand,1,"F",0,NULL,0,0,0,0,0},
    {"readwrite",readwriteCommand,1,"F",0,NULL,0,0,0,0,0},
    {"dump",dumpCommand,2,"r",0,NULL,1,1,1,0,0},
    {"object",objectCommand,3,"r",0,NULL,2,2,2,0,0},
    {"memory",memoryCommand,-2,"r",0,NULL,0,0,0,0,0},
    {"client",clientCommand,-2,"as",0,NULL,0,0,0,0,0},
    {"eval",evalCommand,-3,"s",0,evalGetKeys,0,0,0,0,0},
    {"evalsha",evalShaCommand,-3,"s",0,evalGetKeys,0,0,0,0,0},
    {"slowlog",slowlogCommand,-2,"a",0,NULL,0,0,0,0,0},
    {"script",scriptCommand,-2,"s",0,NULL,0,0,0,0,0},
    {"time",timeCommand,1,"RF",0,NULL,0,0,0,0,0},
    {"bitop",bitopCommand,-4,"wm",0,NULL,2,-1,1,0,0},
    {"bitcount",bitcountCommand,-2,"r",0,NULL,1,1,1,0,0},
    {"bitpos",bitposCommand,-3,"r",0,NULL,1,1,1,0,0},
    {"wait",waitCommand,3,"s",0,NULL,0,0,0,0,0},
    {"command",commandCommand,0,"lt",0,NULL,0,0,0,0,0},
    {"geoadd",geoaddCommand,-5,"wm",0,NULL,1,1,1,0,0},
    {"georadius",georadiusCommand,-6,"w",0,NULL,1,1,1,0,0},
    {"georadiusbymember",georadiusByMemberCommand,-5,"w",0,NULL,1,1,1,0,0},
    {"geohash",geohashCommand,-2,"r",0,NULL,1,1,1,0,0},
    {"geopos",geoposCommand,-2,"r",0,NULL,1,1,1,0,0},
    {"geodist",geodistCommand,-4,"r",0,NULL,1,1,1,0,0},
    {"pfselftest",pfselftestCommand,1,"a",0,NULL,0,0,0,0,0},
    {"pfadd",pfaddCommand,-2,"wmF",0,NULL,1,1,1,0,0},
    {"pfcount",pfcountCommand,-2,"r",0,NULL,1,-1,1,0,0},
    {"pfmerge",pfmergeCommand,-2,"wm",0,NULL,1,-1,1,0,0},
    {"pfdebug",pfdebugCommand,-3,"w",0,NULL,0,0,0,0,0},
    {"post",securityWarningCommand,-1,"lt",0,NULL,0,0,0,0,0},
    {"host:",securityWarningCommand,-1,"lt",0,NULL,0,0,0,0,0},
    {"latency",latencyCommand,-2,"aslt",0,NULL,0,0,0,0,0}
};


void serverLogRaw(int level, const char *msg){
    const int syslogLevelMap[] = {LOG_DEBUG, LOG_INFO, LOG_NOTICE, LOG_WARNING};
    const char *c = ".-*#";
    FILE *fp;
    char buf[64];
    int rawmode = (level & LL_RAW);
    int log_to_stdout = server.logfile[0] == '\0';

    level &= 0xff;
    if(level < server.verbosity) return;
    fp  = log_to_stdout ? stdout : fopen(server.logfile,"a");
    if(!fp) return;

    if(rawmode){
        fprintf(fp,"%s",msg); 
    }else{
        int off; 
        struct timeval tv;
        int role_char;
        pid_t pid = getpid();
        gettimeofday(&tv,NULL);
        off = strftime(buf,sizeof(buf),"%d %b %H:%M:%S.",localtime(&tv.tv_sec));
        snprintf(buf+off, sizeof(buf) - off, "%03d",(int)tv.tv_usec/1000);
        if(server.sentinel_mode){
            role_char = 'X'; 
        }else if(pid != server.pid){
            role_char = 'C'; 
        }else{
            role_char = (server.masterhost ? 'S' : 'M'); 
        }
        fprintf(fp, "%d:%c %s %c %s\n", (int)getpid(),role_char,buf,c[level],msg);
    }
    fflush(fp);
    if(!log_to_stdout) fclose(fp);
    if(server.syslog_enabled) syslog(syslogLevelMap[level],"%s",msg);
};


void serverLog(int level, const char *fmt,...){
    va_list ap;
    char msg[LOG_MAX_LEN];
    if((level & 0xff) < server.verbosity){
        return; 
    }
    va_start(ap,fmt);
    vsnprintf(msg,sizeof(msg),fmt,ap);
    va_end(ap);
    serverLogRaw(level,msg);
};

void serverLogFromHandler(int level, const char *msg){
    int fd;
    int log_to_stdout = server.logfile[0] == '\0';
    char buf[64];

    if((level & 0xff) < server.verbosity || (log_to_stdout && server.daemonize)){
        return; 
    }

    fd = log_to_stdout ? STDOUT_FILENO : open(server.logfile, O_APPEND | O_CREAT | O_WRONLY, 0644);
    
    if(fd == -1) return;
    
    ll2string(buf,sizeof(buf),getpid());
    if(write(fd, buf,strlen(buf)) == -1) goto err;
    if(write(fd,":signal-handler (",17) == -1) goto err;
    ll2string(buf,sizeof(buf),time(NULL));
    if(write(fd, buf,strlen(buf)) == -1) goto err;
    if(write(fd,") ",2) == -1) goto err;
    if(write(fd,msg,strlen(msg)) == -1) goto err;
    if(write(fd,"\n",1) == -1) goto err;

err:
    if(!log_to_stdout) close(fd);
};


long long ustime(void){
    struct timeval tv;
    long long ust;

    gettimeofday(&tv,NULL);
    ust = ((long long)tv.tv_sec) * 1000000;
    ust += tv.tv_usec;
    return ust;
}

mstime_t mstime(void){
    return ustime() / 1000;
};


void exitFromChild(int retcode){
#ifdef COVERAGE_TEST
    exit(retcode);
#else
    _exit(retcode);
#endif
};


void dictVanillaFree(void *privdata, void *val){
    DICT_NOTUSED(privdata);
    zfree(val);
};


void dictListDestructor(void *privdata, void *val){
    DICT_NOTUSED(privdata);
    listRelease((list*)val);
};

int dictSdsKeyCompare(void *privdata, const void *key1, const void *key2){
    int l1, l2;
    DICT_NOTUSED(privdata);
    l1 = sdslen((sds)key1);
    l2 = sdslen((sds)key2);
    if(l1 != l2) return 0;
    return memcmp(key1,key2,l1) == 0;
};

int dictSdsKeyCaseCompare(void *privdata, const void *key1, const void *key2){
    DICT_NOTUSED(privdata);
    return strcasecmp(key1,key2) == 0;
};

void dictObjectDestructor(void *privdata, void *val){
    DICT_NOTUSED(privdata);
    if(val == NULL) return;
    decrRefCount(val);
};

void dictSdsDestructor(void *privdata, void *val){
    DICT_NOTUSED(privdata);
    sdsfree(val);
};

int dictObjKeyCompare(void *privdata, const void *key1, const void *key2){
    const robj *o1 = key1, *o2 = key2;
    return dictSdsKeyCompare(privdata, o1->ptr, o2->ptr);
};

uint64_t dictObjHash(const void *key){
    const robj *o = key;
    return dictGenHashFunction(o->ptr,sdslen((sds)o->ptr));
};

uint64_t dictSdsHash(const void *key){
    return dictGenHashFunction((unsigned char*)key, sdslen((char*)key));
};

uint64_t dictSdsCaseHash(const void *key){
    return dictGenCaseHashFunction((unsigned char*)key,sdslen((char*)key));
};

int dictEncObjKeyCompare(void *privdata, const void *key1, const void *key2){
    robj *o1 = (robj*) key1, *o2 = (robj*) key2; 
    int cmp;

    if(o1->encoding == OBJ_ENCODING_INT &&
       o2->encoding == OBJ_ENCODING_INT){
        return o1->ptr == o2->ptr; 
    };
    
    o1 = getDecodedObject(o1);
    o2 = getDecodedObject(o2);

    cmp = dictSdsKeyCompare(privdata,o1->ptr,o2->ptr);
    decrRefCount(o1);
    decrRefCount(o2);
    return cmp;
};

uint64_t dictEncObjHash(const void *key){
    robj *o = (robj*) key;

    if(sdsEncodedObject(o)){
        return dictGenHashFunction(o->ptr,sdslen((sds)o->ptr)); 
    }else{
        if(o->encoding == OBJ_ENCODING_INT){
           char buf[32];  
           int len;


           len = ll2string(buf,32,(long)o->ptr);
           return dictGenHashFunction((unsigned char*)buf,len);
        }else{
            uint64_t hash; 

            o = getDecodedObject(o);
            hash = dictGenHashFunction(o->ptr,sdslen((sds)o->ptr));
            decrRefCount(o); 
            return hash;
        }
    }
};


dictType objectKeyPointerValueDictType = {
    dictEncObjHash,
    NULL,
    NULL,
    dictEncObjKeyCompare,
    dictObjectDestructor,
    NULL
};

dictType setDictType = {
    dictSdsHash,
    NULL,
    NULL,
    dictSdsKeyCompare,
    dictSdsDestructor,
    NULL
};

dictType zsetDictType = {
    dictSdsHash,
    NULL,
    NULL,
    dictSdsKeyCompare,
    NULL,
    NULL
};

dictType dbDictType = {
    dictSdsHash,
    NULL,
    dictSdsKeyCompare,
    dictSdsDestructor,
    dictObjectDestructor
};

dictType shaScriptObjectDictType = {
    dictSdsCaseHash,
    NULL,
    NULL,
    dictSdsKeyCaseCompare,
    dictSdsDestructor,
    dictObjectDestructor
};

dictType keyptrDictType = {
    dictSdsHash,
    NULL,
    NULL,
    dictSdsKeyCompare,
    NULL,
    NULL
};

dictType commandTableDictType = {
    dictSdsCaseHash,
    NULL,
    NULL,
    dictSdsKeyCompare,
    dictSdsDestructor,
    NULL
};


dictType hashDictType = {
    dictSdsHash,
    NULL,
    NULL,
    dictSdsKeyCompare,
    dictSdsDestructor,
    dictSdsDestructor
};

dictType keylistDictType = {
   dictObjHash,
   NULL,
   NULL, 
   dictObjKeyCompare,
   dictObjectDestructor,
   dictListDestructor
};


dictType clusterNodesDictType = {
    dictSdsHash,
    NULL,
    NULL,
    dictSdsKeyCompare,
    dictSdsDestructor,
    NULL
};

dictType clusterNodesBlackListDictType = {
    dictSdsHash,
    NULL,
    NULL,
    dictSdsKeyCompare,
    dictSdsDestructor,
    NULL
};

dictType modulesDictType = {
    dictSdsCaseHash,
    NULL,
    NULL,
    dictSdsKeyCaseCompare,
    dictSdsDestructor,
    NULL
};

dictType migrateCacheDictType = {
    dictSdsHash,
    NULL,
    NULL,
    dictSdsKeyCompare,
    dictSdsDestructor,
    NULL
};


dictType replScriptCacheDictType = {
    dictSdsCaseHash,
    NULL,
    NULL,
    dictSdsKeyCaseCompare,
    dictSdsDestructor,
    NULL
}; 

int htNeedsResize(dict *dict){
    long long size, used;
    size = dictSlots(dict);
    used = dictSize(dict);

    return (size > DICT_HT_INITIAL_SIZE &&
            (used * 100/size < HASHTABLE_MIN_FILL) 
            );
};

void tryResizeHashTables(int dbid){
    if(htNeedsResize(server.db[dbid].dict)){
        dictResize(server.db[dbid].dict); 
    };

    if(htNeedsResize(server.db[dbid].expires)){
        dictResize(server.db[dbid].expires); 
    };
};

int incrementallyRehash(int dbid){
    if(dictIsRehashing(server.db[dbid].dict)){
        dictRehashMilliseconds(server.db[dbid].dict,1);  
        return 1;
    };

    if(dictIsRehashing(server.db[dbid].expires)){
        dictRehashMilliseconds(server.db[dbid].dict,1); 
        return 1;
    };

    if(dictIsRehashing(server.db[dbid].expires)){
        dictRehashMilliseconds(server.db[dbid].expires,1); 
        return 1;
    };

    return 0;
};

void updateDictResizePolicy(void){
    if(server.rdb_child_pid == -1 && server.aof_child_pid == -1){
        dictEnableResize(); 
    }else{
        dictDisableResize(); 
    }
};

void trackInstantaneousMetric(int metric, long long current_reading){
    long long t = mstime() - server.inst_metric[metric].last_sample_time;
    long long ops = current_reading - server.inst_metric[metric].last_sample_count;

    long long ops_sec;

    ops_sec = t > 0 ? (ops * 1000/t) : 0;

    server.inst_metric[metric].samples[server.inst_metric[metric].idx] = ops_sec;
    server.inst_metric[metric].idx++;
    server.inst_metric[metric].idx %= STATS_METRIC_SAMPLES;
    server.inst_metric[metric].last_sample_time = mstime();
    server.inst_metric[metric].last_sample_count = current_reading;
};


long long getInstantaneousMetric(int metric){
    int j;
    long long sum = 0;
    for(j = 0; j < STATS_METRIC_SAMPLES;j++){
        sum += server.inst_metric[metric].samples[j]; 
    };
    return sum/STATS_METRIC_SAMPLES;

};



int clientsCronHandleTimeout(client *c, mstime_t now_ms){
    time_t now = now_ms / 1000;

    if(server.maxidletime &&
       !(c->flags & CLIENT_SLAVE) &&
       !(c->flags & CLIENT_MASTER) &&
       !(c->flags & CLIENT_BLOCKED) &&
       !(c->flags & CLIENT_PUBSUB) &&
       (now - c->lastinteraction > server.maxidletime)){
        serverLog(LL_VERBOSE, "Closing idle client");  
        freeClient(c);
        return 1;
    }else if(c->flags & CLIENT_BLOCKED){
        if(c->bpop.timeout != 0 && c->bpop.timeout < now_ms){
            replyToBlockedClientTimedOut(c); 
            unblockClient(c);
        }else if(server.cluster_enabled){
            if(clusterRedirectBlockedClientIfNeeded(c)){
                unblockClient(c); 
            }
        }
    }

    return 0;
};



int clientsCronResizeQueryBuffer(client *c){
    size_t querybuf_size = sdsAllocSize(c->querybuf);
    time_t idletime = server.unixtime - c->lastinteraction;

    if(((querybuf_size > PROTO_MBULK_BIG_ARG) && 
       (querybuf_size/(c->querybuf_peak + 1)) > 2) 
       ||(querybuf_size > 1024 && idletime > 2)){
        if(sdsavail(c->querybuf) > 1024){
            c->querybuf = sdsRemoveFreeSpace(c->querybuf); 
        } 
    }
    
    c->querybuf_peak = 0; 
    return 0;
};

#define CLIENTS_CRON_MIN_ITERATIONS 5


void clientsCron(void){
    int numclients = listLength(server.clients);
    int iterations = numclients / server.hz;
    mstime_t now = mstime();
    
    if(iterations < CLIENTS_CRON_MIN_ITERATIONS){
        iterations = (numclients < CLIENTS_CRON_MIN_ITERATIONS) ? numclients : CLIENTS_CRON_MIN_ITERATIONS; 
    };


    while(listLength(server.clients) && iterations--){
       client *c;
       listNode *head; 

       listRotate(server.clients);
       head = listFirst(server.clients); 
       c = listNodeValue(head);


       if(clientsCronHandleTimeout(c,now)) continue;
       if(clientsCronResizeQueryBuffer(c)) continue;
    };
};


void databasesCron(void){
    if(server.active_expire_enabled && server.masterhost ==NULL){
        activeExpireCycle(ACTIVE_EXPIRE_CYCLE_SLOW); 
    }else if(server.masterhost != NULL){
        expireSlaveKeys(); 
    };

    if(server.active_defrag_enabled){
        activeDefragCycle(); 
    }

    if(server.rdb_child_pid == -1 && server.aof_child_pid == -1){
        static unsigned int resize_db = 0;
        static unsigned int rehash_db = 0; 
        int dbs_per_call = CRON_DBS_PER_CALL;
        int j;

        if(dbs_per_call > server.dbnum) dbs_per_call = server.dbnum;

        for(j = 0; j < dbs_per_call; j++){
            tryResizeHashTables(resize_db % server.dbnum); 
            resize_db++;
        };
        
        if(server.activerehashing){
            for(j = 0; j < dbs_per_call; j++){
                int work_done = incrementallyRehash(rehash_db % server.dbnum); 
                rehash_db++;
                if(work_done){
                    break; 
                };
            }; 
        };
    }; 
};


void updateCachedTime(void){
    server.unixtime = time(NULL);
    server.mstime = mstime();
}

int serverCron(struct aeEventLoop *eventLoop, long long id, void *clientData){
    int j;
    UNUSED(eventLoop);
    UNUSED(id);
    UNUSED(clientData);


    if(server.watchdog_period) watchdogScheduleSignal(server.watchdog_period);

    updateCachedTime();

    run_with_period(100){
        trackInstantaneousMetric(STATS_METRIC_COMMAND,server.stat_numcommands); 
        trackInstantaneousMetric(STATS_METRIC_NET_INPUT,server.stat_net_input_bytes);
        trackInstantaneousMetric(STATS_METRIC_NET_OUTPUT,server.stat_net_output_bytes);
    }

    server.lruclock = getLRUClock(); 
    
    if(zmalloc_used_memory() > server.stat_peak_memory){
        server.stat_peak_memory = zmalloc_used_memory(); 
    };

   server.resident_set_size = zmalloc_get_rss();

   if(server.shutdown_asap){
        if(prepareForShutdown(SHUTDOWN_NOFLAGS) == C_OK) exit(0); 
        serverLog(LL_WARNING, "SIGTERM received but errors trying to shut down the server, check the logs for more information");
        server.shutdown_asap = 0;
   };

   if(!server.sentinel_mode){
        run_with_period(5000){
            serverLog(LL_VERBOSE, "%lu clients connected (%lu slaves), %zu bytes in use", 
                    listLength(server.clients) - listLength(server.slaves),
                    listLength(server.slaves), 
                    zmalloc_used_memory()
                    ); 
        }; 
   }; 

   clientsCron();

   databasesCron();

   if(server.rdb_child_pid == -1 && server.aof_child_pid == -1 && server.aof_rewrite_scheduled){
        rewriteAppendOnlyFileBackground(); 
   }

   if(server.rdb_child_pid != -1 || server.aof_child_pid != -1 || ldbPendingChildren()){
        int statloc;
        pid_t pid;
        if((pid = wait3(&statloc, WNOHANG,NULL)) != 0){
           int exitcode = WEXITSTATUS(statloc);  
           int bysignal = 0;
           if(WIFSIGNALED(statloc)) bysignal = WTERMSIG(statloc); 

           if(pid == -1){
                serverLog(LL_WARNING, "wait3() returned an error: %s. rdb_child_pid = %d, aof_child_pid = %d",strerror(errno),(int) server.rdb_child_pid, (int) server.aof_child_pid); 
           }else if(pid == server.rdb_child_pid){
                backgroundSaveDoneHandler(exitcode,bysignal); 
                if(!bysignal && exitcode == 0) receiveChildInfo();
           }else if(pid == server.aof_child_pid){
                backgroundRewriteDoneHandler(exitcode, bysignal); 
                if(!bysignal && exitcode == 0) receiveChildInfo();
           }else{
                if(!ldbRemoveChild(pid)){
                    serverLog(LL_WARNING, "Warning, detected child with unmatched pid: %ld", (long)pid); 
                } 
           }

           updateDictResizePolicy();
           closeChildInfoPipe(); 
        };
   }else{
        for(j = 0; j < server.saveparamslen; j++){
            struct saveparam *sp = server.saveparams + j; 
            
            if(server.dirty >= sp->changes &&
               server.unixtime - server.lastsave > sp->seconds &&
               (server.unixtime - server.lastbgsave_try > CONFIG_BGSAVE_RETRY_DELAY ||
                server.lastbgsave_status == C_OK
                )){
                serverLog(LL_NOTICE, "%d changes in %d seconds. Saving...", sp->changes, (int)sp->seconds); 
                rdbSaveBackground(server.rdb_filename,NULL);
                break;
            };            
        };         

        if(server.rdb_child_pid == -1 && 
           server.aof_child_pid == -1 &&     
           server.aof_rewrite_perc &&
           server.aof_current_size > server.aof_rewrite_min_size     
                ){
            long long base = server.aof_rewrite_base_size ? server.aof_rewrite_base_size : 1; 
            long long growth = (server.aof_current_size * 100 / base) - 100; 
            if(growth >= server.aof_rewrite_perc){
                serverLog(LL_NOTICE, "Starting automatic rewriting of AOF on %lld%% growth",growth); 
                rewriteAppendOnlyFileBackground();
            };  
        };
   }; 

  if(server.aof_flush_postponed_start) flushAppendOnlyFile(0); 
    
  run_with_period(1000){
     if(server.aof_last_write_status == C_ERR){
        flushAppendOnlyFile(0); 
     } 
  }


  freeClientsInAsyncFreeQueue();

  clientsArePaused();

  run_with_period(100){
        if(server.sentinel_mode) sentinelTimer(); 
  }


  run_with_period(1000){
        migrateCloseTimedoutSockets(); 
  }
   
  if(server.rdb_child_pid == -1 && server.aof_child_pid == -1 && 
         server.rdb_bgsave_scheduled &&
         (server.unixtime - server.lastbgsave_try > CONFIG_BGSAVE_RETRY_DELAY || server.lastbgsave_status == C_OK)){
        if(rdbSaveBackground(server.rdb_filename,NULL) == C_OK){
            server.rdb_bgsave_scheduled = 0; 
        } 
  }; 

  server.cronloops++;
  return 1000/server.hz;
};


void beforeSleep(struct aeEventLoop *eventLoop){
    UNUSED(eventLoop);

    if(server.cluster_enabled) clusterBeforeSleep();
   
    if(server.active_expire_enabled && server.masterhost == NULL){
        activeExpireCycle(ACTIVE_EXPIRE_CYCLE_FAST); 
    };  

    if(server.get_ack_from_slaves){
        robj *argv[3]; 

        argv[0] = createStringObject("REPLCONF",8);
        argv[1] = createStringObject("GETACK",6);
        argv[2] = createStringObject("*",1);
        replicationFeedSlaves(server.slaves,server.slaveseldb,argv,3);
        decrRefCount(argv[0]);
        decrRefCount(argv[1]);
        decrRefCount(argv[3]);
        server.get_ack_from_slaves  = 0;
    };


    if(listLength(server.clients_waiting_acks)){
        processClientsWaitingReplicas(); 
    };

    moduleHandleBlockedClients();

    if(listLength(server.unblocked_clients)){
        processUnblockedClients(); 
    };

    flushAppendOnlyFile(0);
    handleClientsWithPendingWrites();
};


void createSharedObjects(void){
    int j;

    shared.crlf = createObject(OBJ_STRING, sdsnew("\r\n")); 
    shared.ok = createObject(OBJ_STRING,sdsnew("+OK\r\n"));
    shared.err = createObject(OBJ_STRING,sdsnew("-ERR\r\n"));
    shared.emptybulk = createObject(OBJ_STRING, sdsnew("$0\r\n"));
    shared.czero = createObject(OBJ_STRING,sdsnew(":0\r\n"));
    shared.cone = createObject(OBJ_STRING,sdsnew(":1\r\n"));
    shared.cnegone = createObject(OBJ_STRING, sdsnew(":-1\r\n"));
    shared.nullbulk = createObject(OBJ_STRING,sdsnew("$-1\r\n"));
    shared.nullmultibulk = createObject(OBJ_STRING,sdsnew("*-1\r\n"));
    shared.emptymultibulk = createObject(OBJ_STRING,sdsnew("*0\r\n"));
    shared.pong = createObject(OBJ_STRING,sdsnew("+PONG\r\n"));
    shared.queued = createObject(OBJ_STRING, sdsnew("+QUEUED\r\n"));
    shared.emptyscan = createObject(OBJ_STRING,sdsnew("*2\r\n$1\r\n0\r\n*0\r\n"));
    shared.wrongtypeerr = createObject(OBJ_STRING, sdsnew(
        "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n" 
    ));
    shared.nokeyerr = createObject(OBJ_STRING, sdsnew(
        "-ERR no such key\r\n"
                ));
    shared.syntaxerr = createObject(OBJ_STRING,sdsnew(
        "-ERR syntax error\r\n" 
                ));
    shared.sameobjecterr = createObject(OBJ_STRING,sdsnew(
        "-ERR source and destination objects are the same\r\n" 
                ));
    shared.outofrangeerr = createObject(OBJ_STRING, sdsnew(
        "-ERR index out of range \r\n" 
                ));
    shared.noscripterr = createObject(OBJ_STRING,sdsnew(
        "-NOSCRIPT No matching script. Please use EVAL. \r\n"        
                ));
    shared.loadingerr = createObject(OBJ_STRING,sdsnew(
        "-LOADING Redis is loading the dataset in memory.\r\n" 
                ));
    shared.slowscripterr = createObject(OBJ_STRING, sdsnew(
        "-BUSY Redis is busy running a script. You can only call SCRIPT KILL or SHUTDOWN NOSAVE. \r\n"        
                ));
    shared.masterdownerr = createObject(OBJ_STRING,sdsnew(
        "MASTERDOWN Link with MASTER is down and slave-serve-stale-data is set to 'no' \r\n"        
                ));
    shared.bgsaveerr = createObject(OBJ_STRING, sdsnew(
        "-MISCONF Redis is configured to save RDB snapshots, but is currently not able to persist on disk. Commands that may modify the data set are disabled. Please check Redis logs for details about the error.\r\n"));
    shared.roslaveerr = createObject(OBJ_STRING,sdsnew( 
       "-READONLY You can't write against a read only slave.\r\n"
                ));
    shared.noautherr = createObject(OBJ_STRING,sdsnew(
                "-NOAUTH Authentication required.\r\n"
                ));
    shared.oomerr = createObject(OBJ_STRING,sdsnew(
                "-OOM command not allowed when used memory > 'maxmemory'.\r\n"
                ));
    shared.execaborterr = createObject(OBJ_STRING,sdsnew(
                "-EXECABORT Transaction discarded because of previous errors.\r\n"
                ));
    shared.noreplicaserr = createObject(OBJ_STRING,sdsnew(
         "-NOREPLICAS Not enough good slaves to write.\r\n"
                ));
    shared.busykeyerr = createObject(OBJ_STRING,sdsnew(
                "-BUSYKEY Target key name already exists.\r\n"
                ));
    shared.space = createObject(OBJ_STRING, sdsnew(
              " "  
                ));
    shared.colon = createObject(OBJ_STRING,sdsnew(":"));
    shared.plus = createObject(OBJ_STRING,sdsnew("+"));

    for(j = 0; j < PROTO_SHARED_SELECT_CMDS; j++){
        char dictid_str[64]; 
        int dictid_len;

        dictid_len = ll2string(dictid_str, sizeof(dictid_str),j);
        shared.select[j] = createObject(OBJ_STRING, sdscatprintf(sdsempty(),
                   "*2\r\n$6\r\nSELECT\r\n$%d\r\n%s\r\n", 
                   dictid_len,dictid_str
                    ));
    };
        shared.messagebulk = createStringObject("$7\r\nmessage\r\n",13);
        shared.pmessagebulk = createStringObject("$8\r\npmessage\r\n",14);
        shared.subscribebulk = createStringObject("$9\r\nsubscribe\r\n",15);
        shared.unsubscribebulk = createStringObject("$11\r\nunsubscribe\r\n",18);
        shared.psubscribebulk = createStringObject("$10\r\npsubscribe\r\n",17);
        shared.punsubscribebulk = createStringObject("$12\r\npunsubscribe\r\n",19);
        shared.del = createStringObject("DEL",3);
        shared.unlink = createStringObject("UNLINK",6);
        shared.rpop = createStringObject("RPOP",4);
        shared.lpop = createStringObject("LPUSH",5);

        for(j = 0; j < OBJ_SHARED_INTEGERS; j++){
            shared.integers[j] = makeObjectShared(createObject(OBJ_STRING, (void *)(long)j)); 
            shared.integers[j]->encoding = OBJ_ENCODING_INT;
        };

        for(j = 0; j < OBJ_SHARED_BULKHDR_LEN;j++){
            shared.mbulkhdr[j] = createObject(OBJ_STRING, sdscatprintf(sdsempty(), "*%d\r\n",j)); 
            shared.bulkhdr[j] = createObject(OBJ_STRING, sdscatprintf(sdsempty(),"$%d\r\n",j));
        };

        shared.minstring = sdsnew("minstring");
        shared.maxstring = sdsnew("maxstring");
}


void initServerConfig(void){
    int j;
    
    getRandomHexChars(server.runid,CONFIG_RUN_ID_SIZE);
    server.runid[CONFIG_RUN_ID_SIZE] = '\0';
    changeReplicationId();
    clearReplicationId2();

    server.configfile = NULL;
    server.executable = NULL;

    server.hz = CONFIG_DEFAULT_HZ;
    server.arch_bits = (sizeof(long) == 8) ? 64 : 32;

    server.port = CONFIG_DEFAULT_SERVER_PORT;
    server.tcp_backlog = CONFIG_DEFAULT_TCP_BACKLOG;
    server.bindaddr_count = 0;
    server.unixsocket = NULL;
    server.unixsocketperm = CONFIG_DEFAULT_UNIX_SOCKET_PERM;
    server.ipfd_count = 0;
    server.sofd = -1;
    server.protected_mode = CONFIG_DEFAULT_PROTECTED_MODE;
    server.dbnum = CONFIG_DEFAULT_DBNUM;
    server.verbosity = CONFIG_DEFAULT_VERBOSITY;
    server.maxidletime = CONFIG_DEFAULT_CLIENT_TIMEOUT;

    server.tcpkeepalive = CONFIG_DEFAULT_TCP_KEEPALIVE;
    server.active_expire_enabled = 1;
    server.active_defrag_enabled = CONFIG_DEFAULT_ACTIVE_DEFRAG;
    
    server.active_defrag_ignore_bytes = CONFIG_DEFAULT_DEFRAG_IGNORE_BYTES; 
    server.active_defrag_threshold_lower = CONFIG_DEFAULT_DEFRAG_THRESHOLD_LOWER;
    server.active_defrag_threshold_upper = CONFIG_DEFAULT_DEFRAG_THRESHOLD_UPPER;
    server.active_defrag_cycle_min = CONFIG_DEFAULT_DEFRAG_CYCLE_MIN;
    server.active_defrag_cycle_max = CONFIG_DEFAULT_DEFRAG_CYCLE_MAX;

    server.client_max_querybuf_len = PROTO_MAX_QUERYBUF_LEN;

    server.saveparams = NULL;
    server.loading = 0;
    server.logfile = zstrdup(CONFIG_DEFAULT_LOGFILE);
    server.syslog_enabled = CONFIG_DEFAULT_SYSLOG_ENABLED;
    server.syslog_ident = zstrdup(CONFIG_DEFAULT_SYSLOG_IDENT); 
    server.syslog_facility = LOG_LOCAL0;
    server.daemonize = CONFIG_DEFAULT_DAEMONIZE;
    server.supervised = 0;
    server.supervised_mode = SUPERVISED_NONE;
    server.aof_state = AOF_OFF;
    server.aof_fsync = CONFIG_DEFAULT_AOF_FSYNC;
    server.aof_no_fsync_on_rewrite = CONFIG_DEFAULT_AOF_NO_FSYNC_ON_REWRITE;
    server.aof_rewrite_perc = AOF_REWRITE_PERC;
    server.aof_rewrite_min_size = AOF_REWRITE_MIN_SIZE;
    server.aof_rewrite_base_size = 0;
    server.aof_rewrite_scheduled = 0;
    server.aof_last_fsync = time(NULL);
    server.aof_rewrite_time_last = -1;
    server.aof_rewrite_time_start = -1;
    server.aof_lastbgrewrite_status = C_OK;
    server.aof_delayed_fsync = 0;
    server.aof_fd = -1;
    server.aof_selected_db = -1;
    server.aof_flush_postponed_start = 0;
    server.aof_rewrite_incremental_fsync = CONFIG_DEFAULT_AOF_REWRITE_INCREMENTAL_FSYNC;
    server.aof_load_truncated = CONFIG_DEFAULT_AOF_LOAD_TRUNCATED;
    server.aof_use_rdb_preamble = CONFIG_DEFAULT_AOF_USE_RDB_PREAMBLE;
    server.pidfile = NULL;
    server.rdb_filename = zstrdup(CONFIG_DEFAULT_RDB_FILENAME);
    server.aof_filename = zstrdup(CONFIG_DEFAULT_AOF_FILENAME);
    server.requirepass = NULL;


    server.rdb_compression = CONFIG_DEFAULT_RDB_COMPRESSION;
    server.rdb_checksum = CONFIG_DEFAULT_RDB_CHECKSUM;

    server.stop_writes_on_bgsave_err = CONFIG_DEFAULT_STOP_WRITES_ON_BGSAVE_ERROR;
    server.activerehashing = CONFIG_DEFAULT_ACTIVE_REHASHING;

    server.active_defrag_running = 0;
    server.notify_keyspace_events = 0; 

    server.maxclients = CONFIG_DEFAULT_MAX_CLIENTS;
    server.bpop_blocked_clients = 0;

    server.maxmemory = CONFIG_DEFAULT_MAXMEMORY;
    server.maxmemory_policy = CONFIG_DEFAULT_MAXMEMORY_POLICY;
    server.maxmemory_samples = CONFIG_DEFAULT_MAXMEMORY_SAMPLES;

    server.lfu_log_factor = CONFIG_DEFAULT_LFU_LOG_FACTOR;
    server.lfu_decay_time = CONFIG_DEFAULT_LFU_DECAY_TIME;

    server.hash_max_ziplist_entries = OBJ_HASH_MAX_ZIPLIST_ENTRIES;
    server.hash_max_ziplist_value = OBJ_HASH_MAX_ZIPLIST_VALUE;

    server.list_max_ziplist_size = OBJ_LIST_MAX_ZIPLIST_SIZE;
    server.list_compress_depth = OBJ_LIST_COMPRESS_DEPTH;
    server.set_max_intset_entries = OBJ_SET_MAX_INTSET_ENTRIES;

    server.zset_max_ziplist_entries = OBJ_ZSET_MAX_ZIPLIST_ENTRIES;
    server.zset_max_ziplist_value = OBJ_ZSET_MAX_ZIPLIST_VALUE;

    server.hll_sparse_max_bytes = CONFIG_DEFAULT_HLL_SPARSE_MAX_BYTES;
    server.shutdown_asap = 0;
    server.cluster_enabled = 0;
    server.cluster_node_timeout = CLUSTER_DEFAULT_NODE_TIMEOUT;
    server.cluster_migration_barrier = CLUSTER_DEFAULT_NODE_TIMEOUT;
    server.cluster_slave_validity_factor = CLUSTER_DEFAULT_SLAVE_VALIDITY;
    server.cluster_require_full_coverage = CLUSTER_DEFAULT_REQUIRE_FULL_COVERAGE;
    server.cluster_configfile = zstrdup(CONFIG_DEFAULT_CLUSTER_CONFIG_FILE);
    server.cluster_announce_ip = CONFIG_DEFAULT_CLUSTER_ANNOUNCE_IP;
    server.cluster_announce_port = CONFIG_DEFAULT_CLUSTER_ANNOUNCE_PORT;
    server.cluster_announce_bus_port = CONFIG_DEFAULT_CLUSTER_ANNOUNCE_BUS_PORT;
    server.migrate_cached_sockets = dictCreate(&migrateCacheDictType,NULL);
    server.next_client_id = 1;
    server.loading_process_events_interval_bytes = (1024 * 1024 * 2);
    server.lazyfree_lazy_eviction = CONFIG_DEFAULT_LAZYFREE_LAZY_EVICTION;
    server.lazyfree_lazy_expire = CONFIG_DEFAULT_LAZYFREE_LAZY_EXPIRE;
    server.lazyfree_lazy_server_del = CONFIG_DEFAULT_LAZYFREE_LAZY_SERVER_DEL;
    server.always_show_logo = CONFIG_DEFAULT_ALWAYS_SHOW_LOGO;

    server.lruclock = getLRUClock();
    resetServerSaveParams();


    appendServerSaveParams(60 * 60, 1);
    appendServerSaveParams(300,100);
    appendServerSaveParams(60,10000);
    
    server.masterauth = NULL;
    server.masterhost = NULL;
    server.masterport = 6397;
    
    server.master = NULL;
    server.cached_master = NULL;
    server.master_initial_offset = -1;
    server.repl_state = REPL_STATE_NONE;
    server.repl_syncio_timeout = CONFIG_REPL_SYNCIO_TIMEOUT;
    server.repl_serve_stale_data = CONFIG_DEFAULT_SLAVE_SERVE_STALE_DATA;
    server.repl_slave_ro = CONFIG_DEFAULT_SLAVE_READ_ONLY;
    server.repl_slave_lazy_flush = CONFIG_DEFAULT_SLAVE_LAZY_FLUSH;
    server.repl_down_since = 0;
    server.repl_disable_tcp_nodelay = CONFIG_DEFAULT_REPL_DISABLE_TCP_NODELAY;
    server.repl_diskless_sync = CONFIG_DEFAULT_REPL_DISKLESS_SYNC;
    server.repl_diskless_sync_delay = CONFIG_DEFAULT_REPL_DISKLESS_SYNC_DELAY;
    server.slave_announce_ip = CONFIG_DEFAULT_SLAVE_ANNOUNCE_IP;
    server.slave_announce_port = CONFIG_DEFAULT_SLAVE_ANNOUNCE_PORT;
    server.master_repl_offset = 0; 


    server.repl_backlog = NULL;
    server.repl_backlog_size = CONFIG_DEFAULT_REPL_BACKLOG_SIZE;
    server.repl_backlog_histlen = 0;
    server.repl_backlog_idx = 0;
    server.repl_backlog_off = 0;
    server.repl_backlog_time_limit = CONFIG_DEFAULT_REPL_BACKLOG_TIME_LIMIT;
    server.repl_no_slaves_since = time(NULL);


    for(j = 0; j < CLIENT_TYPE_OBUF_COUNT;j++){
        server.client_obuf_limits[j] = clientBufferLimitsDefaults[j]; 
    };

    R_Zero = 0.0;
    R_PosInf = 1.0 / R_Zero;
    R_NegInf = -1.0 / R_Zero;
    
    R_Nan = R_Zero / R_Zero;

    server.commands = dictCreate(&commandTableDictType, NULL);
    server.orig_commands = dictCreate(&commandTableDictType,NULL);
    populateCommandTable();
    server.delCommand = lookupCommandByCString("del");
    server.multiCommand = lookupCommandByCString("multi");
    server.lpushCommand = lookupCommandByCString("lpush");
    server.lpopCommand = lookupCommandByCString("lpop");
    server.rpopCommand = lookupCommandByCString("srem");
    server.execCommand = lookupCommandByCString("exec");

    server.slowlog_log_slower_than = CONFIG_DEFAULT_SLOWLOG_LOG_SLOWER_THAN;
    server.slowlog_max_len  = CONFIG_DEFAULT_SLOWLOG_MAX_LEN;

    server.latency_monitor_threshold = CONFIG_DEFAULT_LATENCY_MONITOR_THRESHOLD;

    server.assert_failed = "<no assertion failed>";
    server.assert_file = "<no file>";

    server.assert_line = 0;
    server.bug_report_start = 0;
    server.watchdog_period = 0;
}


extern char **environ;

int restartServer(int flags, mstime_t delay){
    int j;

    if(access(server.executable, X_OK) == -1) return C_ERR;

    if(flags & RESTART_SERVER_CONFIG_REWRITE && server.configfile && rewriteConfig(server.configfile) == -1){
        return C_ERR; 
    }
    
    if(flags & RESTART_SERVER_GRACEFULLY && prepareForShutdown(SHUTDOWN_NOFLAGS) != C_OK){
        return C_ERR; 
    };

    for(j = 3;j < (int) server.maxclients + 1024; j++){
        close(j); 
    };

    if(delay)usleep(delay * 1000);
     
    execve(server.executable, server.exec_argv, environ); 
    _exit(1);
    return C_ERR;
}


void adjustOpenFilesLimit(void){
    rlim_t maxfiles = server.maxclients + CONFIG_MIN_RESERVED_FDS;
    struct rlimit limit;

    if(getrlimit(RLIMIT_NOFILE,&limit) == -1){
        serverLog(LL_WARNING, "Unable to obtain the current NOFILE limit (%s), assuming 1024 and setting the max clients configuration accordingly", strerror(errno)); 
        server.maxclients = 1024 - CONFIG_MIN_RESERVED_FDS;
    }else{
        rlim_t oldlimit = limit.rlim_cur;
        
        if(oldlimit < maxfiles){
            rlim_t bestlimit;
            int setrlimit_error = 0; 

            bestlimit = maxfiles;
            while(bestlimit > oldlimit){
                rlim_t decr_step = 16; 

                limit.rlim_cur = bestlimit;
                limit.rlim_max = bestlimit;

                if(setrlimit(RLIMIT_NOFILE,&limit) != -1) break;
                setrlimit_error = errno;
                
                if(bestlimit < decr_step)break; 
                bestlimit -= decr_step;
            }; 
            
            if(bestlimit < oldlimit) bestlimit = oldlimit;

            if(bestlimit < maxfiles){
                unsigned int old_maxclients = server.maxclients;
                server.maxclients = bestlimit - CONFIG_MIN_RESERVED_FDS; 
                
                if(bestlimit <= CONFIG_MIN_RESERVED_FDS){
                    serverLog(LL_WARNING, "Your current 'ulimit -n' of %llu is not enough for the"
                                " server to start. Please increase your open file limit to at least " 
                            " %llu. Exiting.", (unsigned long long)oldlimit, (unsigned long long)maxfiles);   
                     exit(1); 
                };

                serverLog(LL_WARNING, "You requested maxclients of %d requiring at least %llu max file descriptors.", old_maxclients, (unsigned long long) maxfiles);
                serverLog(LL_WARNING, "Server can't set maximum open files to %llu because of OS error: %s.",(unsigned long long)maxfiles, strerror(setrlimit_error));
                serverLog(LL_WARNING,"Current maximum open files is %llu. maxclients has been reduced to %d to compensate for low ulimit. If you need higher maxclients increase 'ulimit -n'.",(unsigned long long)bestlimit, server.maxclients);
            }else{
               serverLog(LL_NOTICE, "Increased maximum number of open files to %llu (it was originally set to %llu).",(unsigned long long)maxfiles, (unsigned long long) oldlimit);    
            };
        }
    }
}


void checkTcpBacklogSettings(void){
#ifdef HAVE_PROC_SOMAXCONN
    FILE *fp = fopen("/proc/sys/net/core/somaxconn","r"); 
    char buf[1024];
    if(!fp) return;
    if(fgets(buf,sizeof(buf),fp) != NULL){
        int somaxconn = atoi(buf); 
        if(somaxconn > 0 && somaxconn < server.tcp_backlog){
            serverLog(LL_WARNING,"WARNING: The TCP backlog setting of %d cannot be enforced because /proc/sys/net/core/somaxconn is set to the lower value of %d.", server.tcp_backlog, somaxconn);          
        }
    };
    fclose(fp);
#endif
}

int listenToPort(int port, int *fds, int *count){
    int j;

    if(server.bindaddr_count == 0) server.bindaddr[0] = NULL;
    for(j = 0; j < server.bindaddr_count || j == 0; j++){
        if(server.bindaddr[j] == NULL){
            int unsupported = 0;
            fds[*count] = anetTcp6Server(server.neterr, port, NULL,server.tcp_backlog); 
            
            if(fds[*count] != ANET_ERR){
                anetNonBlock(NULL, fds[*count]); 
                (*count)++;
            }else if(errno == EAFNOSUPPORT){
                unsupported++; 
                serverLog(LL_WARNING, "Not listening to IPv6: unsupported");
            }; 
            
            if(*count == 1 && unsupported){
                fds[*count] = anetTcpServer(server.neterr, port, NULL, server.tcp_backlog); 
                if(fds[*count] != ANET_ERR){
                    anetNonBlock(NULL,fds[*count]); 
                    (*count)++;
                }else if(errno == EAFNOSUPPORT){
                    unsupported++; 
                    serverLog(LL_WARNING, "Not listening to IPV4: unsupported");
                }
            };    
                 
            if(*count + unsupported == 2) break; 
        }else if(strchr(server.bindaddr[j],':')){
            fds[*count] = anetTcp6Server(server.neterr, port, server.bindaddr[j],server.tcp_backlog); 
        }else{
            fds[*count] = anetTcpServer(server.neterr, port, server.bindaddr[j], server.tcp_backlog); 
        };        

        if(fds[*count] == ANET_ERR){
            serverLog(LL_WARNING, "Creating Server TCP listening socket %s:%d: %s", server.bindaddr[j] ? server.bindaddr[j] : "*", port, server.neterr); 
            return C_ERR;
        }

        anetNonBlock(NULL, fds[*count]);
        (*count)++;
    };
     return C_OK;
}


void resetServerStats(void){
    int j;

    server.stat_numcommands = 0;
    server.stat_numconnections = 0;
    server.stat_expiredkeys = 0;
    server.stat_evictedkeys = 0;
    server.stat_keyspace_misses = 0;
    server.stat_keyspace_hits = 0;
    server.stat_active_defrag_hits = 0;
    server.stat_active_defrag_misses = 0;
    server.stat_active_defrag_key_hits = 0;
    server.stat_active_defrag_key_misses = 0;
    server.stat_fork_time = 0;
    server.stat_fork_rate = 0;
    server.stat_sync_full = 0;
    server.stat_rejected_conn = 0;
    server.stat_rejected_conn = 0;
    server.stat_sync_full = 0;
    server.stat_sync_partial_ok = 0;
    server.stat_sync_partial_err = 0;

    for(int j = 0; j < STATS_METRIC_COUNT; j++){
        server.inst_metric[j].idx = 0;
        server.inst_metric[j].last_sample_time = mstime(); 
        server.inst_metric[j].last_sample_count = 0;
        memset(server.inst_metric[j].samples, 0, sizeof(server.inst_metric[j].samples));
    };

    server.stat_net_input_bytes = 0;
    server.stat_net_output_bytes = 0;
    server.aof_delayed_fsync = 0;
}


int initServer(void){
    int j;
    signal(SIGHUP, SIG_IGN);
    signal(SIGPIPE,SIG_IGN);
    
    if(server.syslog_enabled){
        openlog(server.syslog_ident, LOG_PID | LOG_NDELAY | LOG_NOWAIT, server.syslog_facility); 
    }

    server.pid= getpid();
    server.current_client = NULL;
    server.clients = listCreate();
    server.clients_to_close = listCreate();
    server.slaves = listCreate();
    server.monitors = listCreate();
    server.clients_pending_write = listCreate();
    server.slaveseldb = -1;
    server.unblocked_clients = listCreate();
    server.ready_keys = listCreate();
    server.clients_waiting_acks = listCreate();
    server.get_ack_from_slaves = 0;
    server.system_memory_size = zmalloc_get_memory_size();

    createSharedObjects();
    adjustOpenFilesLimit();
    server.el = aeCreateEventLoop(server.maxclients + CONFIG_FDSET_INCR);
    server.db = zmalloc(sizeof(redisDb) *server.dbnum);

    if(server.port != 0 && listenToPort(server.port, server.ipfd, &server.ipfd_count) == C_ERR){
        exit(1); 
    };


    if(server.unixsocket != NULL){
        unlink(server.unixsocket); 
        server.sofd = anetUnixServer(server.neterr, server.unixsocket, server.unixsocketperm, server.tcp_backlog);
        if(server.sofd == ANET_ERR){
            serverLog(LL_WARNING, "Opening Unix socket: %s",server.neterr); 
            exit(1);
        }
        anetNonBlock(NULL,server.sofd);
    };

    if(server.ipfd_count == 0 && server.sofd < 0){
        serverLog(LL_WARNING, "Configured to not listen anywhere, exiting"); 
        exit(1);
    }

    for(j = 0; j < server.dbnum; j++){
       server.db[j].dict = dictCreate(&dbDictType, NULL);          
       server.db[j].expires = dictCreate(&keyptrDictType, NULL);
       server.db[j].blocking_keys = dictCreate(&keylistDictType,NULL);
       server.db[j].ready_keys = dictCreate(&objectKeyPointerValueDictType,NULL);
       server.db[j].watched_keys = dictCreate(&keylistDictType,NULL);
       server.db[j].id = j;
       server.db[j].avg_ttl = 0;
    };
    
    evictionPoolAlloc();
    server.pubsub_channels = dictCreate(&keylistDictType,NULL);
    server.pubsub_patterns = listCreate();
    listSetFreeMethod(server.pubsub_patterns, freePubsubPattern);
    listSetMatchMethod(server.pubsub_patterns, listMatchPubsubPattern);
    server.cronloops = 0;
    server.rdb_child_pid = -1;
    server.aof_child_pid = -1;
    server.rdb_child_type = RDB_CHILD_TYPE_NONE;
    server.rdb_bgsave_scheduled = 0;
    server.child_info_pipe[0] = -1;
    server.child_info_pipe[1] = -1;
    server.child_info_data.magic = 0;
    aofRewriteBufferReset();
    server.aof_buf = sdsempty();
    server.lastsave = time(NULL); 
    server.lastbgsave_try = 0;
    server.rdb_save_time_last = -1;
    server.rdb_save_time_start = -1;
    server.dirty = 0;
    resetServerStats();
    server.stat_starttime = time(NULL);
    server.stat_peak_memory = 0;
    server.stat_rdb_cow_bytes = 0;
    server.stat_aof_cow_bytes = 0;
    server.resident_set_size = 0;
    server.lastbgsave_status = C_OK;
    server.aof_last_write_status = C_OK;
    server.aof_last_write_errno = 0;
    server.repl_good_slaves_count = 0;
    updateCachedTime();

    if(aeCreateTimeEvent(server.el, 1, serverCron, NULL, NULL) == AE_ERR){
        serverPanic("Can't create event loop timers."); 
        exit(1);
    };

    for(j = 0; j < server.ipfd_count; j++){
        if(aeCreateFileEvent(server.el, server.ipfd[j],AE_READABLE, acceptTcpHandler,NULL) == AE_ERR){
            serverPanic("Unrecoverable error creating server.ipfd file event."); 
        }; 
    } 

    if(server.sofd > 0 && aeCreateFileEvent(server.el, server.sofd, AE_READABLE, acceptUnixHandler,NULL) == AE_ERR){
        serverPanic("Unrecoverable error creating server.sofd file event."); 
    };

    if(server.arch_bits == 32 && server.maxmemory == 0){
        serverLog(LL_WARNING, "Warning: 32 bit instance detected but no memory limit set. Setting 3GB max memory limit with 'no eviction' policy now."); 
        server.maxmemory = 3072LL * (1024 * 1024);
        server.maxmemory_policy = MAXMEMORY_NO_EVICTION; 
    }

    if(server.cluster_enabled) clusterInit();
    replicationScriptCacheInit();
    scriptingInit(1);
    slowlogInit();
    latencyMonitorInit();
    bioInit();
    server.initial_memory_usage = zmalloc_used_memory();
};

void populateCommandTable(void){
    int j;
    int numcommands = sizeof(redisCommandTable) / sizeof(struct redisCommand);
    
    for(j = 0; j < numcommands; j++){
        struct redisCommand *c = redisCommandTable + j; 
        char *f = c->sflags;
        int retval1, retval2;

        while(*f != '\0'){
            switch(*f){
                case 'w': c->flags |= CMD_WRITE; break;
                case 'r': c->flags |= CMD_READONLY;break; 
                case 'm': c->flags |= CMD_DENYOOM; break;
                case 'a': c->flags |= CMD_ADMIN; break;
                case 'p': c->flags |= CMD_PUBSUB;break;
                case 's': c->flags |= CMD_NOSCRIPT;break;
                case 'R': c->flags |= CMD_RANDOM;break;
                case 'S': c->flags |= CMD_SORT_FOR_SCRIPT;break;
                case 'l': c->flags |= CMD_LOADING;break;
                case 't': c->flags |= CMD_STALE;break;
                case 'M': c->flags |= CMD_SKIP_MONITOR;break;
                case 'k': c->flags |= CMD_ASKING;break;
                case 'F': c->flags |= CMD_FAST;break;
                default: serverPanic("Unsupported command flag");break;
            }; 
            f++;
        };
    retval1 = dictAdd(server.commands, sdsnew(c->name),c);
    retval2 = dictAdd(server.orig_commands, sdsnew(c->name),c);
    serverAssert(retval1 == DICT_OK && retval2 == DICT_OK);
   };
};

void resetCommandTableStats(void){
    int numcommands = sizeof(redisCommandTable) / sizeof(struct redisCommand);
    int j;


    for(j = 0; j < numcommands; j++){
        struct redisCommand *c = redisCommandTable+j; 
        c->microseconds = 0;
        c->calls = 0;
    };
}


void redisOpArrayInit(redisOpArray *oa){
    oa->ops = NULL;
    oa->numops = 0;
};

int redisOpArrayAppend(redisOpArray *oa, struct redisCommand *cmd, int dbid, robj **argv, int argc, int target){
     redisOp *op;
     oa->ops = zrealloc(oa->ops, sizeof(redisOp) * (oa->numops + 1));
     op = oa->ops + oa->numops;
     op->cmd = cmd;
     op->dbid = dbid;
     op->argv = argv;
     op->argc = argc;
     op->target = target;
     oa->numops++;
     return oa->numops;
};

void redisOpArrayFree(redisOpArray *oa){
    while(oa->numops){
        int j; 
        redisOp *op;
        
       oa->numops--;
       op = oa->ops + oa->numops; 
       for(j = 0; j < op->argc; j++){
            decrRefCount(op->argv[j]); 
       };
       zfree(op->argv);
    };

    zfree(oa->ops);
};

struct redisCommand *lookupCommand(sds name){
    return dictFetchValue(server.commands,name);
};

struct redisCommand *lookupCommandByCString(char *s){
    struct redisCommand *cmd;
    sds name = sdsnew(s);

    cmd = dictFetchValue(server.commands, name);
    sdsfree(name);
    return cmd;
}


struct redisCommand *lookupCommandOrOriginal(sds name){
    struct redisCommand *cmd = dictFetchValue(server.commands, name);

    if(!cmd) cmd = dictFetchValue(server.orig_commands, name);
    return cmd;
}

void propagate(struct redisCommand *cmd, int dbid, robj **argv, int argc, int flags){
    if(server.aof_state != AOF_OFF && flags & PROPAGATE_AOF){
        feedAppendOnlyFile(cmd, dbid, argv, argc); 
    }

    if(flags & PROPAGATE_REPL){
        replicationFeedSlaves(server.slaves, dbid, argv, argc); 
    }
};



void alsoPropagate(struct redisCommand *cmd, int dbid, robj **argv, int argc, int target){
    robj **argvcopy;
    int j;  


    if(server.loading) return;

    argvcopy = zmalloc(sizeof(robj*) * argc);
    
    for(j = 0; j < argc; j++){
        argvcopy[j] = argv[j]; 
        incrRefCount(argv[j]);
    };

    redisOpArrayAppend(&server.also_propagate, cmd, dbid, argvcopy, argc, target);
};


void forceCommandPropagation(client *c, int flags){
    if(flags & PROPAGATE_REPL) c->flags |= CLIENT_FORCE_REPL;
    if(flags & PROPAGATE_AOF) c->flags |= CLIENT_FORCE_AOF;
};

void preventCommandPropagation(client *c){
    c->flags |= CLIENT_PREVENT_PROP;
};

void preventCommandAOF(client *c){
    c->flags |= CLIENT_PREVENT_AOF_PROP;
};

void preventCommandReplication(client *c){
    c->flags |= CLIENT_PREVENT_REPL_PROP;
};


void call(client *c, int flags){
    long long dirty, start, duration;
    int client_old_flags = c->flags;
    
    if(listLength(server.monitors) && !server.loading && !(c->cmd->flags & (CMD_SKIP_MONITOR | CMD_ADMIN))){
        replicationFeedMonitors(c,server.monitors, c->db->id, c->argv, c->argc); 
    }; 

    c->flags &= ~(CLIENT_FORCE_AOF | CLIENT_FORCE_REPL | CLIENT_PREVENT_PROP);
    
    redisOpArray prev_also_propagate = server.also_propagate;
    redisOpArrayInit(&server.also_propagate);

    dirty = server.dirty;
    start = ustime();
    c->cmd->proc(c);
    duration = ustime() - start;
    dirty = server.dirty - dirty;
    if(dirty < 0) dirty = 0;

    if(server.loading && c->flags & CLIENT_LUA){
        flags &= ~( CMD_CALL_SLOWLOG | CMD_CALL_STATS ); 
    }
    
    if(c->flags & CLIENT_LUA && server.lua_caller){
        if(c->flags & CLIENT_FORCE_REPL){
            server.lua_caller->flags |= CLIENT_FORCE_REPL; 
        } 

        if(c->flags & CLIENT_FORCE_AOF){
            server.lua_caller->flags |= CLIENT_FORCE_AOF; 
        };
    };

    if(flags & CMD_CALL_SLOWLOG && c->cmd->proc != execCommand){
        char *latency_event = (c->cmd->flags & CMD_FAST) ? "fast-command" : "command"; 
        latencyAddSampleIfNeeded(latency_event, duration/1000);
        slowlogPushEntryIfNeeded(c->argv, c->argc, duration);
    };

    if(flags & CMD_CALL_STATS){
        c->lastcmd->microseconds += duration;
        c->lastcmd->calls++; 
    }

    if(flags & CMD_CALL_PROPAGATE && (c->flags & CLIENT_PREVENT_PROP) != CLIENT_PREVENT_PROP){
        int propagate_flags = PROPAGATE_NONE; 

        if(dirty) propagate_flags |= (PROPAGATE_AOF | PROPAGATE_REPL);
            
        if(c->flags & CLIENT_FORCE_REPL) propagate_flags |= PROPAGATE_REPL;
        if(c->flags & CLIENT_FORCE_AOF) propagate_flags |= PROPAGATE_AOF;
        
        if(c->flags & CLIENT_PREVENT_REPL_PROP || !(flags & CMD_CALL_PROPAGATE_REPL)){
            propagate_flags &= ~PROPAGATE_REPL; 
        } 

        if(c->flags & CLIENT_PREVENT_AOF_PROP || !(flags & CMD_CALL_PROPAGATE_AOF)){
            propagate_flags &= ~PROPAGATE_AOF; 
        }

        if(propagate_flags != PROPAGATE_NONE){
            propagate(c->cmd, c->db->id, c->argv, c->argc,propagate_flags); 
        };
    } 

    c->flags &= ~(CLIENT_FORCE_AOF | CLIENT_FORCE_REPL | CLIENT_PREVENT_PROP);
    c->flags |= client_old_flags & (CLIENT_FORCE_AOF|CLIENT_FORCE_REPL|CLIENT_PREVENT_PROP);

    if(server.also_propagate.numops){
        int j;
        redisOp *rop; 

        if(flags & CMD_CALL_PROPAGATE){
           for(j = 0; j < server.also_propagate.numops; j++){
                rop = &server.also_propagate.ops[j];   
                int target = rop->target;
                if(!(flags & CMD_CALL_PROPAGATE_AOF)) target &= ~PROPAGATE_AOF;
                if(!(flags & CMD_CALL_PROPAGATE_REPL)) target &= ~PROPAGATE_REPL;
                if(target){
                    propagate(rop->cmd, rop->dbid, rop->argv,rop->argc,target); 
                } 
           };  
        };
        redisOpArrayFree(&server.also_propagate);
    }; 

    server.also_propagate = prev_also_propagate;
    server.stat_numcommands++;
};



int processCommand(client *c){
    if(!strcasecmp(c->argv[0]->ptr,"quit")){
        addReply(c,shared.ok); 
        c->flags |= CLIENT_CLOSE_AFTER_REPLY;
        return C_ERR;
   }; 

    c->cmd = c->lastcmd = lookupCommand(c->argv[0]->ptr); 
    if(!c->cmd){
        flagTransaction(c); 
        addReplyErrorFormat(c,"unknown command %s", (char*)c->argv[0]->ptr);
        return C_OK;
    }else if((c->cmd->arity > 0 && c->cmd->arity !=  c->argc) || (c->argc < -c->cmd->arity)){
        flagTransaction(c); 
        addReplyErrorFormat(c,"wrong number of arguments for '%s' command", c->cmd->name);
        return C_OK;
    };

    if(server.requirepass && !c->authenticated && c->cmd->proc != authCommand){
        flagTransaction(c); 
        addReply(c,shared.noautherr);
        return C_OK;
    };
    
    if(server.cluster_enabled && !(c->flags & CLIENT_MASTER) && 
            !(c->flags & CLIENT_LUA && server.lua_caller->flags & CLIENT_MASTER) && 
            !(c->cmd->getkeys_proc == NULL && c->cmd->firstkey == 0 && c->cmd->proc != execCommand)){
       int hashslot;
       int error_code;
        
       clusterNode *n = getNodeByQuery(c,c->cmd,c->argv,c->argc,&hashslot,&error_code);  
       if(n == NULL || n != server.cluster->myself){
            if(c->cmd->proc == execCommand){
                discardTransaction(c); 
            }else{
                flagTransaction(c); 
            } 
            clusterRedirectClient(c,n,hashslot,error_code);
            return C_OK;
       };
    }; 

    if(server.maxmemory){
        int retval = freeMemoryIfNeeded(); 
        if(server.current_client == NULL) return C_ERR;
        
        if((c->cmd->flags & CMD_DENYOOM) && retval == C_ERR){
            flagTransaction(c); 
            addReply(c,shared.oomerr);
            return C_OK;
        }; 
    };

    
    if(((server.stop_writes_on_bgsave_err && 
         server.saveparamslen > 0 && 
         server.lastbgsave_status == C_ERR) || server.aof_last_write_status == C_ERR) &&        
         server.masterhost == NULL && 
         (c->cmd->flags & CMD_WRITE || c->cmd->proc == pingCommand)){
            flagTransaction(c); 
            if(server.aof_last_write_status == C_OK){
                addReply(c,shared.bgsaveerr); 
            }else{
                addReplySds(c,sdscatprintf(sdsempty(),"-MISCONF Errors writing to the AOF file: %s\r\n",
                           strerror(server.aof_last_write_errno))); 
            }

            return C_OK;
    } 

    if(server.masterhost == NULL &&
       server.repl_min_slaves_to_write &&
       server.repl_min_slaves_max_lag &&
       c->cmd->flags & CMD_WRITE && 
       server.repl_good_slaves_count < server.repl_min_slaves_to_write 
            ){
        flagTransaction(c); 
        addReply(c,shared.noreplicaserr);
        return C_OK;
    };
    
    if(server.masterhost && server.repl_slave_ro && 
       !(c->flags & CLIENT_MASTER) && c->cmd->flags & CMD_WRITE 
            ){
        addReply(c,shared.roslaveerr); 
        return C_OK;
    };

    if(c->flags & CLIENT_PUBSUB &&
       c->cmd->proc != pingCommand &&
       c->cmd->proc != subscribeCommand &&
       c->cmd->proc != unsubscribeCommand &&
       c->cmd->proc != psubscribeCommand &&
       c->cmd->proc != punsubscribeCommand 
            ){
        addReplyError(c,"only (P)SUBCRIBE / (P)UNSUBSCRIBE / PING / QUIT allowed in this context");
        return C_OK; 
    };  


    if(server.masterhost && server.repl_state != REPL_STATE_CONNECTED && server.repl_serve_stale_data){
        flagTransaction(c); 
        addReply(c,shared.masterdownerr);
        return C_OK;
    };

    if(server.loading && !(c->cmd->flags & CMD_LOADING)){
        addReply(c,shared.loadingerr); 
        return C_OK;
    };

    if(server.lua_timedout &&
       c->cmd->proc != authCommand &&  
       c->cmd->proc != replconfCommand &&
       !(c->cmd->proc == shutdownCommand &&
         c->argc == 2 &&
         tolower(((char*)c->argv[1]->ptr)[0]) == 'n') && 
       !(c->cmd->proc == scriptCommand && 
         c->argc == 2 &&
         tolower(((char*)c->argv[1]->ptr)[0]) == 'k' 
        )){
            flagTransaction(c); 
            addReply(c,shared.slowscripterr);
            return C_OK;
    };

    if(c->flags & CLIENT_MULTI &&
       c->cmd->proc != execCommand && c->cmd->proc != discardCommand &&
       c->cmd->proc != multiCommand && c->cmd->proc != watchCommand){
        queueMultiCommand(c); 
        addReply(c,shared.queued);
    }else{
        call(c,CMD_CALL_FULL); 
        c->woff = server.master_repl_offset;
        if(listLength(server.ready_keys)){
            handleClientsBlockedOnLists(); 
        };
    }

    return C_OK;
};


void closeListeningSockets(int unlink_unix_socket){
    int j;

    for(j = 0; j < server.ipfd_count;j++){
        close(server.ipfd[j]); 
    };

    if(server.cluster_enabled){
        for(j = 0; j < server.cfd_count; j++){
            close(server.cfd[j]); 
        } 
    }
    
    if(unlink_unix_socket && server.unixsocket){
        serverLog(LL_NOTICE,"Removing the unix socket file."); 
        unlink(server.unixsocket);
    };
};

int prepareForShutdown(int flags){
    int save = flags & SHUTDOWN_SAVE;
    int nosave = flags & SHUTDOWN_NOSAVE;

    serverLog(LL_WARNING, "User requested shutdown...");

    ldbKillForkedSessions();
    
    if(server.rdb_child_pid != -1){
        serverLog(LL_WARNING, "There is a child saving an .rdb. killing it!"); 
        kill(server.rdb_child_pid, SIGUSR1);
        rdbRemoveTempFile(server.rdb_child_pid); 
    } 


    if(server.aof_state != AOF_OFF){
        if(server.aof_child_pid != -1){
            if(server.aof_state == AOF_WAIT_REWRITE){
                serverLog(LL_WARNING,"Writing initial AOF, can't exit.");  
                return C_ERR; 
            }; 

            serverLog(LL_WARNING, "There is a child rewriting the AOF. Killing it!");
            kill(server.aof_child_pid,SIGUSR1); 
        }; 
        serverLog(LL_NOTICE,"Calling fsync() on the AOF file");
        aof_fsync(server.aof_fd);
    };

    if((server.saveparamslen > 0 && !nosave) || save){
        serverLog(LL_NOTICE, "Saving the final RDB snapshot before exiting."); 
        if(rdbSave(server.rdb_filename, NULL) != C_OK){
            serverLog(LL_WARNING,"Error trying to save the DB, can't exit."); 
            return C_ERR;
        };
    };

    if(server.daemonize || server.pidfile){
        serverLog(LL_NOTICE, "Removing the pid file."); 
        unlink(server.pidfile);
    };

    flushSlavesOutputBuffers();

    closeListeningSockets(1);

    serverLog(LL_WARNING, "%s is now ready to exit, bye bye...", server.sentinel_mode ? "Sentinel" : "Redis");
    return C_OK;
};

int time_independent_strcmp(char *a, char *b){
    char bufa[CONFIG_AUTHPASS_MAX_LEN], bufb[CONFIG_AUTHPASS_MAX_LEN];
    unsigned int alen = strlen(a);
    unsigned int blen = strlen(b);
    unsigned int j;
    int diff = 0;
    
    if(alen > sizeof(bufa) || blen > sizeof(bufb)) return 1;

    memset(bufa, 0, sizeof(bufa));
    memset(bufb, 0, sizeof(bufb));

    memcpy(bufa, a, alen);
    memcpy(bufb, b, blen);

    for(j = 0; j < sizeof(bufa); j++){
        diff |= (bufa[j] ^ bufb[j]); 
    };
    
    diff |= alen ^ blen;
    return diff;
};

void authCommand(client *c){
    if(!server.requirepass){
        addReplyError(c,"Client sent AUTH, but no password is set"); 
    }else if(!time_independent_strcmp(c->argv[1]->ptr, server.requirepass)){
        c->authenticated = 1; 
        addReply(c,shared.ok);
    }else{
        c->authenticated = 0; 
        addReplyError(c,"invalid password");
    }
};

void pingCommand(client *c){
    if(c->argc > 2){
        addReplyErrorFormat(c, "wrong number of arguments for '%s' command", c->cmd->name); 
        return;
    }
    
    if(c->flags & CLIENT_PUBSUB){
        addReply(c,shared.mbulkhdr[2]); 
        addReplyBulkCBuffer(c,"pong",4);
        if(c->argc == 1){
            addReplyBulkCBuffer(c,"",0); 
        }else{
            addReplyBulk(c,c->argv[1]); 
        }
    }else{
        if(c->argc == 1){
            addReply(c,shared.pong); 
        }else{
            addReplyBulk(c,c->argv[1]); 
        } 
    }
};


void echoCommand(client *c){
    addReplyBulk(c,c->argv[1]);
};


void timeCommand(client *c){
    struct timeval tv;

    gettimeofday(&tv, NULL);
    addReplyMultiBulkLen(c,2);
    addReplyBulkLongLong(c,tv.tv_sec);
    addReplyBulkLongLong(c,tv.tv_usec);
};


int addReplyCommandFlag(client *c, struct redisCommand *cmd, int f, char *reply){
    if(cmd->flags & f){
        addReplyStatus(c, reply); 
        return 1;
    }
    return 0;
};


void addReplyCommand(client *c, struct redisCommand *cmd){
    if(!cmd){
        addReply(c,shared.nullbulk); 
    }else{
        addReplyMultiBulkLen(c,6); 
        addReplyBulkCString(c,cmd->name);
        addReplyLongLong(c,cmd->arity);


        int flagcount = 0;
        void *flaglen = addDeferredMultiBulkLength(c);
        flagcount += addReplyCommandFlag(c,cmd,CMD_WRITE,"write");
        flagcount += addReplyCommandFlag(c,cmd,CMD_READONLY,"readonly");
        flagcount += addReplyCommandFlag(c,cmd,CMD_DENYOOM,"denyoom");
        flagcount += addReplyCommandFlag(c,cmd,CMD_ADMIN,"admin");
        flagcount += addReplyCommandFlag(c,cmd,CMD_PUBSUB,"pubsub");
        flagcount += addReplyCommandFlag(c,cmd,CMD_NOSCRIPT,"noscript");
        flagcount += addReplyCommandFlag(c,cmd,CMD_RANDOM,"random");
        flagcount += addReplyCommandFlag(c,cmd,CMD_SORT_FOR_SCRIPT,"sort_for_script");
        flagcount += addReplyCommandFlag(c,cmd,CMD_LOADING,"loading");
        flagcount += addReplyCommandFlag(c,cmd,CMD_STALE,"stale");
        flagcount += addReplyCommandFlag(c,cmd,CMD_SKIP_MONITOR,"skip_monitor");
        flagcount += addReplyCommandFlag(c,cmd,CMD_ASKING,"asking");
        flagcount += addReplyCommandFlag(c,cmd,CMD_FAST,"fast");
        if((cmd->getkeys_proc && !(cmd->flags & CMD_MODULE)) || cmd->flags & CMD_MODULE_GETKEYS){
            addReplyStatus(c,"movablekeys"); 
            flagcount += 1;
        }

        setDeferredMultiBulkLength(c,flaglen,flagcount);
        addReplyLongLong(c,cmd->firstkey);
        addReplyLongLong(c,cmd->lastkey);
        addReplyLongLong(c,cmd->keystep);
    }
};


void commandCommand(client *c){
    dictIterator *di;
    dictEntry *de;
    
    if(c->argc == 1){
        addReplyMultiBulkLen(c,dictSize(server.commands)); 
        di = dictGetIterator(server.commands);
        while((de = dictNext(di)) != NULL){
            addReplyCommand(c,dictGetVal(de)); 
        };
        dictReleaseIterator(di);
    }else if(!strcasecmp(c->argv[1]->ptr, "info")){
        int i;
        addReplyMultiBulkLen(c,c->argc-2); 
        for(i = 2; i < c->argc;i++){
            addReplyCommand(c,dictFetchValue(server.commands, c->argv[i]->ptr)); 
        };
    }else if(!strcasecmp(c->argv[1]->ptr,"count") && c->argc == 2){
        addReplyLongLong(c,dictSize(server.commands)); 
    }else if(!strcasecmp(c->argv[1]->ptr,"getkeys") && c->argc >= 3){
        struct redisCommand *cmd = lookupCommand(c->argv[2]->ptr); 
        int *keys, numkeys, j;
        if(!cmd){
            addReplyErrorFormat(c,"Invalid command specified"); 
            return;
        }else if((cmd->arity > 0 && cmd->arity != c->argc - 2) || ((c->argc -2) < -cmd->arity)){
            addReplyError(c,"Invalid number of arguments specified for command"); 
            return;
        }

        keys = getKeysFromCommand(cmd,c->argv+2,c->argc-2,&numkeys);
        addReplyMultiBulkLen(c,numkeys);
        for(j = 0; j< numkeys; j++) addReplyBulk(c,c->argv[keys[j] + 2]);
        getKeysFreeResult(keys);
    }else{
        addReplyError(c,"Unknown subcommand or wrong number of arguments."); 
        return;
    }
};


void bytesToHuman(char *s, unsigned long long n){
    double d;
    if(n<1024){
        sprintf(s,"%lluB",n); 
        return;
    }else if(n < (1024 * 1024)){
        d = (double) n / (1024); 
        sprintf(s,"%.2fK",d);
    }else if(n < (1024LL * 1024 * 1024)){
        d = (double) n / (1024 * 1024); 
        sprintf(s,"%.2fM",d);
    }else if(n < (1024LL * 1024 * 1024 * 1024)){
        d = (double) n / (1024LL * 1024 * 1024); 
        sprintf(s,"%.2fG",d);
    }else if(n < (1024LL * 1024 * 1024 * 1024 * 1024)){
        d = (double) n / (1024LL * 1024 * 1024 * 1024); 
        sprintf(s,"%.2fT",d);
    }else if(n < (1024LL * 1024 * 1024 * 1024 * 1024 * 1024)){
        d = (double) n / (1024LL * 1024 * 1024 * 1024 *1024); 
        sprintf(s,"%.2fP",d);
    }else{
        sprintf(s,"%lluB",n); 
    }
};

sds genRedisInfoString(char *section){
    sds info = sdsempty();
    time_t uptime = server.unixtime - server.stat_starttime;
    int j, numcommands;
    struct rusage self_ru, c_ru;
    unsigned long lol, bib;
    int allsections = 0, defsections = 0;
    int sections = 0;

    if(section == NULL) section = "default";
    allsections = strcasecmp(section,"all") == 0;
    defsections = strcasecmp(section,"default") == 0;

    getrusage(RUSAGE_SELF, &self_ru);
    getrusage(RUSAGE_CHILDREN, &c_ru);
    getClientsMaxBuffers(&lol,&bib);
    
    if(allsections || defsections || !strcasecmp(section,"server")){
        static int call_uname = 1;
        static struct utsname name;
        char *mode;

        if(server.cluster_enabled) {
            mode = "cluster";
        }else if(server.sentinel_mode){
            mode = "sentinel"; 
        }else{
            mode = "standalone"; 
        }
    
        if(sections++) info = sdscat(info,"\r\n"); 

        if(call_uname){
            uname(&name); 
            call_uname = 0;
        }

        info = sdscatprintf(info,
            "# Server\r\n"
            "redis_version:%s\r\n"
            "redis_git_sha1:%s\r\n"
            "redis_git_dirty:%d\r\n"
            "redis_build_id:%llx\r\n"
            "redis_mode:%s\r\n"
            "os:%s %s %s\r\n"
            "arch_bits:%d\r\n"
            "multiplexing_api:%s\r\n"
            "gcc_version:%d.%d.%d\r\n"
            "process_id:%ld\r\n"
            "run_id:%s\r\n"
            "tcp_port:%d\r\n"
            "uptime_in_seconds:%jd\r\n"
            "uptime_in_days:%jd\r\n"
            "hz:%d\r\n"
            "lru_clock:%ld\r\n"
            "executable:%s\r\n"
            "config_file:%s\r\n",
            REDIS_VERSION,
            redisGitSHA1(),
            strtol(redisGitDirty(),NULL,10) > 0,
            (unsigned long long) redisBuildId(),
            mode,
            name.sysname, name.release, name.machine,
            server.arch_bits,
            aeGetApiName(),
#ifdef __GNUC__
            __GNUC__,__GNUC_MINOR__,__GNUC_PATCHLEVEL__,
#else
            0,0,0,
#endif
            (long) getpid(),
            server.runid,
            server.port,
            (intmax_t) uptime,
            (intmax_t) (uptime/(3600 * 24)),
            server.hz,
            (unsigned long) server.lruclock,
            server.executable ? server.executable : "",
            server.configfile ? server.configfile :"");
    }

    if(allsections || defsections || !strcasecmp(section,"clients")){
        if(sections++) info = sdscat(info,"\r\n"); 
        info = sdscatprintf(info,
               "# Clients\r\n" 
               "connected_clients: %lu \r\n"
               "client_longest_output_list:%lu\r\n"
               "client_biggest_input_buf:%lu\r\n"
               "unblocked_clients:%d\r\n",
               listLength(server.clients) - listLength(server.slaves),
               lol,bib,
               server.bpop_blocked_clients);
    };


    if(allsections || defsections || !strcasecmp(section,"memory")){
        char hmem[64]; 
        char peak_hmem[64];
        char total_system_hmem[64];
        char used_memory_lua_hmem[64];
        char used_memory_rss_hmem[64];
        char maxmemory_hmem[64];
        size_t zmalloc_used = zmalloc_used_memory();
        size_t total_system_mem = server.system_memory_size;
        const char *evict_policy = evictPolicyToString();
        long long memory_lua = (long long) lua_gc(server.lua, LUA_GCCOUNT,0) * 1024;
        struct redisMemOverhead *mh = getMemoryOverheadData();

        if(zmalloc_used > server.stat_peak_memory){
            server.stat_peak_memory = zmalloc_used; 
        }

        bytesToHuman(hmem,zmalloc_used);
        bytesToHuman(peak_hmem,server.stat_peak_memory);
        bytesToHuman(total_system_hmem,total_system_mem);
        bytesToHuman(used_memory_lua_hmem,memory_lua);
        bytesToHuman(used_memory_rss_hmem,server.resident_set_size);
        bytesToHuman(maxmemory_hmem,server.maxmemory);

        if(sections++) info = sdscat(info,"\r\n");
        info = sdscatprintf(info,
            "# Memory\r\n"
            "used_memory:%zu\r\n"
            "used_memory_human:%s\r\n"
            "used_memory_rss:%zu\r\n"
            "used_memory_rss_human:%s\r\n"
            "used_memory_peak:%zu\r\n"
            "used_memory_peak_human:%s\r\n"
            "used_memory_peak_perc:%.2f%%\r\n"
            "used_memory_overhead:%zu\r\n"
            "used_memory_startup:%zu\r\n"
            "used_memory_dataset:%zu\r\n"
            "used_memory_dataset_perc:%.2f%%\r\n"
            "total_system_memory:%lu\r\n"
            "total_system_memory_human:%s\r\n"
            "used_memory_lua:%lld\r\n"
            "used_memory_lua_human:%s\r\n"
            "maxmemory:%lld\r\n"
            "maxmemory_human:%s\r\n"
            "maxmemory_policy:%s\r\n"
            "mem_fragmentation_ratio:%.2f\r\n"
            "mem_allocator:%s\r\n"
            "active_defrag_running:%d\r\n"
            "lazyfree_pending_objects:%zu\r\n",
            zmalloc_used,
            hmem,
            server.resident_set_size, 
            used_memory_rss_hmem,
            server.stat_peak_memory,
            peak_hmem,
            mh->peak_perc,
            mh->overhead_total,
            mh->startup_allocated,
            mh->dataset,
            mh->dataset_perc,
            (unsigned long) total_system_mem,
            total_system_hmem,
            memory_lua,
            used_memory_lua_hmem,
            server.maxmemory,
            maxmemory_hmem,
            evict_policy,
            mh->fragmentation,
            ZMALLOC_LIB,
            server.active_defrag_running,
            lazyfreeGetPendingObjectsCount()); 
        
            freeMemoryOverheadData(mh);
    }


    if(allsections || defsections || !strcasecmp(section,"persistence")){
       if(sections++) info = sdscat(info,"\r\n");  
       info = sdscatprintf(info,
             "# Persistence\r\n"
            "loading:%d\r\n"
            "rdb_changes_since_last_save:%lld\r\n"
            "rdb_bgsave_in_progress:%d\r\n"
            "rdb_last_save_time:%jd\r\n"
            "rdb_last_bgsave_status:%s\r\n"
            "rdb_last_bgsave_time_sec:%jd\r\n"
            "rdb_current_bgsave_time_sec:%jd\r\n"
            "rdb_last_cow_size:%zu\r\n"
            "aof_enabled:%d\r\n"
            "aof_rewrite_in_progress:%d\r\n"
            "aof_rewrite_scheduled:%d\r\n"
            "aof_last_rewrite_time_sec:%jd\r\n"
            "aof_current_rewrite_time_sec:%jd\r\n"
            "aof_last_bgrewrite_status:%s\r\n"
            "aof_last_write_status:%s\r\n"
            "aof_last_cow_size:%zu\r\n",
            server.loading,
            server.dirty,
            server.rdb_child_pid != -1,
            (intmax_t)server.lastsave,
            (server.lastbgsave_status == C_OK) ? "ok" : "err",
            (intmax_t)server.rdb_save_time_last,
            (intmax_t)((server.rdb_child_pid == -1) ?
                -1 : time(NULL)-server.rdb_save_time_start),
            server.stat_rdb_cow_bytes,
            server.aof_state != AOF_OFF,
            server.aof_child_pid != -1,
            server.aof_rewrite_scheduled,
            (intmax_t)server.aof_rewrite_time_last,
            (intmax_t)((server.aof_child_pid == -1) ?
                -1 : time(NULL)-server.aof_rewrite_time_start),
            (server.aof_lastbgrewrite_status == C_OK) ? "ok" : "err",
            (server.aof_last_write_status == C_OK) ? "ok" : "err",
            server.stat_aof_cow_bytes);
        if(server.aof_state != AOF_OFF){
            info = sdscatprintf(info,
                "aof_current_size:%lld\r\n"
                "aof_base_size:%lld\r\n"
                "aof_pending_rewrite:%d\r\n"
                "aof_buffer_length:%zu\r\n"
                "aof_rewrite_buffer_length:%lu\r\n"
                "aof_pending_bio_fsync:%llu\r\n"
                "aof_delayed_fsync:%lu\r\n",
                (long long) server.aof_current_size,
                (long long) server.aof_rewrite_base_size,
                server.aof_rewrite_scheduled,
                sdslen(server.aof_buf),
                aofRewriteBufferSize(),
                bioPendingJobsOfType(BIO_AOF_FSYNC),
                server.aof_delayed_fsync); 
            }
        if(server.loading){
            double perc;
            time_t eta, elapsed;
            off_t remaining_bytes = server.loading_total_bytes - server.loading_loaded_bytes;


           perc = ((double) server.loading_loaded_bytes / (server.loading_total_bytes + 1)) * 100; 

           elapsed = time(NULL) - -server.loading_start_time;
           if(elapsed == 0){
                eta = 1; 
           }else{
                eta = (elapsed * remaining_bytes) / (server.loading_loaded_bytes + 1); 
           }
          
          info = sdscatprintf(info,
                "loading_start_time:%jd\r\n"
                "loading_total_bytes:%llu\r\n"
                "loading_loaded_bytes:%llu\r\n"
                "loading_loaded_perc:%.2f\r\n"
                "loading_eta_seconds:%jd\r\n",
                (intmax_t) server.loading_start_time,
                (unsigned long long) server.loading_total_bytes,
                (unsigned long long) server.loading_loaded_bytes,
                perc,
                (intmax_t)eta
            );
            }
       }
    
       if(allsections || defsections || !strcasecmp(section,"stats")){
            if(allsections++) info = sdscat(info, "\r\n"); 
            info = sdscatprintf(info,
            "# Stats\r\n"
            "total_connections_received:%lld\r\n"
            "total_commands_processed:%lld\r\n"
            "instantaneous_ops_per_sec:%lld\r\n"
            "total_net_input_bytes:%lld\r\n"
            "total_net_output_bytes:%lld\r\n"
            "instantaneous_input_kbps:%.2f\r\n"
            "instantaneous_output_kbps:%.2f\r\n"
            "rejected_connections:%lld\r\n"
            "sync_full:%lld\r\n"
            "sync_partial_ok:%lld\r\n"
            "sync_partial_err:%lld\r\n"
            "expired_keys:%lld\r\n"
            "evicted_keys:%lld\r\n"
            "keyspace_hits:%lld\r\n"
            "keyspace_misses:%lld\r\n"
            "pubsub_channels:%ld\r\n"
            "pubsub_patterns:%lu\r\n"
            "latest_fork_usec:%lld\r\n"
            "migrate_cached_sockets:%ld\r\n"
            "slave_expires_tracked_keys:%zu\r\n"
            "active_defrag_hits:%lld\r\n"
            "active_defrag_misses:%lld\r\n"
            "active_defrag_key_hits:%lld\r\n"
            "active_defrag_key_misses:%lld\r\n",
            server.stat_numconnections,
            server.stat_numcommands,
            getInstantaneousMetric(STATS_METRIC_COMMAND),
            server.stat_net_input_bytes,
            server.stat_net_output_bytes,
            (float)getInstantaneousMetric(STATS_METRIC_NET_INPUT)/1024,
            (float)getInstantaneousMetric(STATS_METRIC_NET_OUTPUT)/1024,
            server.stat_rejected_conn,
            server.stat_sync_full,
            server.stat_sync_partial_ok,
            server.stat_sync_partial_err,
            server.stat_expiredkeys,
            server.stat_evictedkeys,
            server.stat_keyspace_hits,
            server.stat_keyspace_misses,
            dictSize(server.pubsub_channels),
            listLength(server.pubsub_patterns),
            server.stat_fork_time,
            dictSize(server.migrate_cached_sockets),
            getSlaveKeyWithExpireCount(),
            server.stat_active_defrag_hits,
            server.stat_active_defrag_misses,
            server.stat_active_defrag_key_hits,
            server.stat_active_defrag_key_misses);
       }; 

       if(allsections || defsections || !strcasecmp(section,"replication")){
            if(sections++) info = sdscat(info, "\r\n"); 
            info = sdscatprintf(info, 
                    "# Replication\r\n"
                    "role:%s\r\n", 
                    server.masterhost == NULL ? "master" : "slave");
            if(server.masterhost){
                long long slave_repl_offset = 1;
                if(server.master){
                    slave_repl_offset = server.master->reploff; 
                }else if(server.cached_master){
                    slave_repl_offset = server.cached_master->reploff; 
                }     
                
                info = sdscatprintf(info,
                "master_host:%s\r\n"
                "master_port:%d\r\n"
                "master_link_status:%s\r\n"
                "master_last_io_seconds_ago:%d\r\n"
                "master_sync_in_progress:%d\r\n"
                "slave_repl_offset:%lld\r\n",
                server.masterhost,
                server.masterport,
                (server.repl_state == REPL_STATE_CONNECTED) ?
                    "up" : "down",
                server.master ?
                ((int)(server.unixtime-server.master->lastinteraction)) : -1,
                server.repl_state == REPL_STATE_TRANSFER,
                slave_repl_offset);

                if(server.repl_state == REPL_STATE_TRANSFER){
                    info = sdscatprintf(info,
                    "master_sync_left_bytes:%lld\r\n"
                    "master_sync_last_io_seconds_ago:%d\r\n"
                    , (long long)
                        (server.repl_transfer_size - server.repl_transfer_read),
                    (int)(server.unixtime-server.repl_transfer_lastio)
                    );
                };

                if(server.repl_state != REPL_STATE_CONNECTED){
                    info = sdscatprintf(info,
                           "master_link_down_since_seconds:%jd\r\n", 
                            (intmax_t)server.unixtime - server.repl_down_since); 
                }  

                info = sdscatprintf(info,
                        "slave_priority:%d\r\n"
                "slave_read_only:%d\r\n",
                server.slave_priority,
                server.repl_slave_ro 
                        ); 
            };

            info = sdscatprintf(info, "connected_slaves:%lu\r\n",listLength(server.slaves));
            
            if(server.repl_min_slaves_to_write && server.repl_min_slaves_max_lag){
                info = sdscatprintf(info, "min_slaves_good_slaves:%d\r\n",server.repl_good_slaves_count);
            };

            if(listLength(server.slaves)){
                 int slaveid = 0;
                 listNode *ln;

                 listIter li;
                 listRewind(server.slaves, &li);
                 while((ln = listNext(&li))){
                    client *slave = listNodeValue(ln); 
                    char *state = NULL;
                    char ip[NET_IP_STR_LEN], *slaveip = slave->slave_ip;
                    int port;
                    long lag = 0;
                    
                    if(slaveip[0] == '\0'){
                        if(anetPeerToString(slave->fd,ip,sizeof(ip),&port)){continue;}; 
                        slaveip = ip;
                    }; 
                    switch(slave->replstate){
                        case SLAVE_STATE_WAIT_BGSAVE_START:
                        case SLAVE_STATE_WAIT_BGSAVE_END: 
                            state = "wait_bgsave";
                            break;
                        case SLAVE_STATE_SEND_BULK:
                           state = "send_bulk"; 
                           break;
                        case SLAVE_STATE_ONLINE:
                           state = "online";
                           break;
                    };

                    if(state == NULL) continue;
                    if(slave->replstate == SLAVE_STATE_ONLINE){
                        lag = time(NULL) - slave->repl_ack_time; 
                    }

                    info = sdscatprintf(info,
                    "slave%d:ip=%s,port=%d,state=%s,"
                    "offset=%lld,lag=%ld\r\n",
                    slaveid,slaveip,slave->slave_listening_port,state,
                    slave->repl_ack_off, lag);
                    slaveid++;
                 };
            };
            info = sdscatprintf(
            "master_replid:%s\r\n"
            "master_replid2:%s\r\n"
            "master_repl_offset:%lld\r\n"
            "second_repl_offset:%lld\r\n"
            "repl_backlog_active:%d\r\n"
            "repl_backlog_size:%lld\r\n"
            "repl_backlog_first_byte_offset:%lld\r\n"
            "repl_backlog_histlen:%lld\r\n",
            server.replid,
            server.replid2,
            server.master_repl_offset,
            server.second_replid_offset,
            server.repl_backlog != NULL,
            server.repl_backlog_size,
            server.repl_backlog_off,
            server.repl_backlog_histlen); 
       };

       if(allsections || defsections || !strcasecmp(section,"cpu")){
            if(sections++) info = sdscat(info, "\r\n"); 
            info = sdscatprintf(info,
        "# CPU\r\n"
        "used_cpu_sys:%.2f\r\n"
        "used_cpu_user:%.2f\r\n"
        "used_cpu_sys_children:%.2f\r\n"
        "used_cpu_user_children:%.2f\r\n",
        (float)self_ru.ru_stime.tv_sec+(float)self_ru.ru_stime.tv_usec/1000000,
        (float)self_ru.ru_utime.tv_sec+(float)self_ru.ru_utime.tv_usec/1000000,
        (float)c_ru.ru_stime.tv_sec+(float)c_ru.ru_stime.tv_usec/1000000,
        (float)c_ru.ru_utime.tv_sec+(float)c_ru.ru_utime.tv_usec/1000000);
       };


       if(allsections || !strcasecmp(section,"commandstats")){
            if(sections++) info = sdscat(info, "\r\n"); 
            info = sdscatprintf(info, "# Commandstats\r\n");
            numcommands = sizeof(redisCommandTable) / sizeof(struct redisCommand);
            for(j = 0; j < numcommands; j++){
                struct redisCommand *c = redisCommandTable + j; 
                if(!c->calls) continue;
                info = sdscatprintf(info,
                          "cmdstat_%s:calls=%lld,usec=%lld,usec_per_call=%.2f\r\n",
                c->name, c->calls, c->microseconds,
                (c->calls == 0) ? 0 : ((float)c->microseconds/c->calls));
            }; 
       };


       if(allsections || defsections || !strcasecmp(section,"cluster")){
            if(sections++) info = sdscat(info, "\r\n"); 
            info = sdscatprintf(info, 
                   "# Cluster \r\n" 
                   "cluster_enabled: %d\r\n",
                   server.cluster_enabled
                    );
       }
    
      if(allsections || defsections || !strcasecmp(section, "keyspace")){
        if(sections++) info = sdscat(info, "\r\n");  
        info = sdscatprintf(info, "# Keyspace \r\n");
        for(j = 0; j < server.dbnum; j++){
            long long keys, vkeys;

            keys = dictSize(server.db[j].dict); 
            vkeys = dictSize(server.db[j].expires);
            if(keys || vkeys){
                info = sdscatprintf(info,   "db%d:keys=%lld,expires=%lld,avg_ttl=%lld\r\n",
                        j, keys, vkeys, server.db[j].avg_ttl 
                        ); 
            
            };
        };
      
      }; 
      return info;
};



void infoCommand(client *c){
    char *section = c->argc == 2 ? c->argv[1]->ptr : "default";

    if(c->argc > 2){
        addReply(c,shared.syntaxerr); 
        return;
    };


    addReplyBulkSds(c,genRedisInfoString(section));
}


void monitorCommand(client *c){
    if(c->flags & CLIENT_SLAVE) return;

    c->flags |= (CLIENT_SLAVE | CLIENT_MONITOR);
    listAddNodeTail(server.monitors,c);
    addReply(c,shared.ok);
};

#ifdef __linux__
int linuxOvercommitMemoryValue(void){
    FILE *fp = fopen("/proc/sys/vm/overcommit_memory","r");
    char buf[64];

    if(!fp) return -1;
    if(fgets(buf,64,fp) == NULL){
        fclose(fp); 
        return -1;
    }

    fclose(fp);
    return atoi(buf);
}; 

void linuxMemoryWarnings(void){
    if(linuxOvercommitMemoryValue() == 0){
          serverLog(LL_WARNING,"WARNING overcommit_memory is set to 0! Background save may fail under low memory condition. To fix this issue add 'vm.overcommit_memory = 1' to /etc/sysctl.conf and then reboot or run the command 'sysctl vm.overcommit_memory=1' for this to take effect.");
    }

    if(THPIsEnabled()){
         serverLog(LL_WARNING,"WARNING you have Transparent Huge Pages (THP) support enabled in your kernel. This will create latency and memory usage issues with Redis. To fix this issue run the command 'echo never > /sys/kernel/mm/transparent_hugepage/enabled' as root, and add it to your /etc/rc.local in order to retain the setting after a reboot. Redis must be restarted after THP is disabled.");
             
    }
}
#endif

void createPidFile(void){
    if(!server.pidfile) server.pidfile = zstrdup(CONFIG_DEFAULT_PID_FILE);

    FILE *fp = fopen(server.pidfile,"w");
    if(fp){
        fprintf(fp, "%d\n",(int)getpid()); 
        fclose(fp);
    }
}

void daemonize(void){
    int fd;
        
    if(fork() != 0 ) exit(0);

    setsid();
    
    if((fd = open("/dev/null",O_RDWR, 0)) != -1){
        dup2(fd,STDIN_FILENO); 
        dup2(fd,STDOUT_FILENO);
        dup2(fd,STDERR_FILENO);
        if(fd > STDERR_FILENO) close(fd);
    }
}

void version(void){
    printf("Redis server v=%s sha=%s:%d malloc = %s bits=%d build=%llx\n",
           REDIS_VERSION,
           redisGitSHA1(),
           atoi(redisGitDirty()) > 0,
           ZMALLOC_LIB,
           sizeof(long) == 4 ? 32 : 64,
           (unsigned long long) redisBuildId());
    exit(0);
}

void usage(void){
    fprintf(stderr,"Usage: ./redis-server [/path/to/redis.conf] [options]\n");
    fprintf(stderr,"       ./redis-server - (read config from stdin)\n");
    fprintf(stderr,"       ./redis-server -v or --version\n");
    fprintf(stderr,"       ./redis-server -h or --help\n");
    fprintf(stderr,"       ./redis-server --test-memory <megabytes>\n\n");
    fprintf(stderr,"Examples:\n");
    fprintf(stderr,"       ./redis-server (run the server with default conf)\n");
    fprintf(stderr,"       ./redis-server /etc/redis/6379.conf\n");
    fprintf(stderr,"       ./redis-server --port 7777\n");
    fprintf(stderr,"       ./redis-server --port 7777 --slaveof 127.0.0.1 8888\n");
    fprintf(stderr,"       ./redis-server /etc/myredis.conf --loglevel verbose\n\n");
    fprintf(stderr,"Sentinel mode:\n");
    fprintf(stderr,"       ./redis-server /etc/sentinel.conf --sentinel\n");
    exit(1);
}

void redisAsciiArt(void){
#include "asciilogo.h"
    char *buf = zmalloc(1024*16);
    char *mode;


    if(server.cluster_enabled) mode = "cluster";
    else if(server.sentinel_mode) mode = "sentinel";
    else mode = "standalone";

    int show_logo = ((!server.syslog_enabled && server.logfile[0] == '\0' && isatty(fileno(stdout))) || server.always_show_logo);

    if(!show_logo){
        serverLog(LL_NOTICE,"Running mode = %s, port = %d.", mode, server.port); 
    }else{
        snprintf(buf,1024*16,ascii_logo,REDIS_VERSION,redisGitSHA1(),strtol(redisGitDirty(),NULL,10) > 0, (sizeof(long) == 8) ? "64" : "32", mode, server.port, (long)getpid()); 
        serverLogRaw(LL_NOTICE || LL_RAW, buf); 
    }; 

   zfree(buf); 
}

static void sigShutdownHandler(int sig){
    char *msg;
    
    switch(sig){
        case SIGINT:
           msg = "Received SIGINT scheduling shutdown..."; 
           break;
        case SIGTERM:
           msg = "Received SIGTERM scheduling shutdown...";
           break;
        default:
           msg = "Received shutdown signal, scheduling shutdown...";
    }
    
    if(server.shutdown_asap && sig == SIGINT){
        serverLogFromHandler(LL_WARNING, "You insist... exiting now."); 
        rdbRemoveTempFile(getpid());
        exit(1);
    }else if(server.loading){
        exit(0);
    };

    serverLogFromHandler(LL_WARNING, msg);
    server.shutdown_asap = 1;
}



void setupSignalHandlers(void){
    struct sigaction act;

    sigemptyset(&act.sa_mask);
    act.sa_flags = 0;
    act.sa_handler = sigShutdownHandler;
    sigaction(SIGTERM, &act, NULL);
    sigaction(SIGINT, &act, NULL);
#ifdef HAVE_BACKTRACE
    sigemptyset(&act.sa_mask);
    act.sa_flags = SA_NODEFER | SA_RESETHAND | SA_SIGINFO;
    act.sa_sigaction = sigsegvHandler;
    sigaction(SIGSEGV, &act, NULL);
    sigaction(SIGBUS, &act, NULL);
    sigaction(SIGFPE,&act,NULL);
    sigaction(SIGILL,&act,NULL);
#endif
    return;
}

void memtest(size_t megabytes, int passes);


int checkForSentinelMode(int argc, char **argv){
    int j;
    if(strstr(argv[0],"redis-sentinel") != NULL) return 1;
    for(j = 1; j < argc; j++){
        if(!strcmp(argv[j],"--sentinel")) return 1; 
    };

    return 0;
};



void loadDataFromDisk(void){
    long long start = ustime();    

    if(server.aof_state == AOF_ON){
        if(loadAppendOnlyFile(server.aof_filename) == C_OK){
            serverLog(LL_NOTICE,"DB loaded from appedn only file: %.3f seconds", (float)(ustime() - start)/1000000); 
        }; 
    }else{
        rdbSaveInfo rsi = RDB_SAVE_INFO_INIT;
        if(rdbLoad(server.rdb_filename,&rsi) == C_OK){
            serverLog(LL_NOTICE,"DB loaded from disk: %.3f seconds", (float)(ustime() - start)/1000000);        
            if(rsi.repl_id_is_set && rsi.repl_offset != -1){
                memcpy(server.replid,rsi.repl_id, sizeof(server.replid)); 
                server.master_repl_offset = rsi.repl_offset;
                if(server.masterhost) replicationCacheMasterUsingMyself();
            }; 
        }else if(errno != ENOENT){
            serverLog(LL_WARNING, "Fatal error loading the DB: %s. Exiting.", strerror(errno)); 
            exit(1);
        } 
    }
}

void redisOutOfMemoryHandler(size_t allocation_size){
    serverLog(LL_WARNING, "Out Of Memory allocating %zu bytes!",allocation_size);
    serverPanic("Redis aborting for OUT OF MEMORY");
}


void redisSetProcTitle(char *title){
#ifdef USE_SETPROCTITLE
    char *server_mode = "";
    if(server.cluster_enabled) server_mode = " [cluster]";
    else if(server.sentinel_mode) server_mode = " [sentinel]";
    
    setproctitle("%s %s:%d%s", title, server.bindaddr_count ? server.bindaddr[0] : "*",
    server.port,
    server_mode);
#else
    UNUSED();
#endif
};

int redisSupervisedUpstart(void){
    const char *upstart_job = getenv("UPSTART_JOB");
    if(!upstart_job){
        serverLog(LL_WARNING,"upstart supervision requested, but UPSTART_JOB not found"); 
        return 0; 
    };

    serverLog(LL_NOTICE,"supervised by upstart, will stop to signal readiness");
    raise(SIGSTOP);
    unsetenv("UPSTART_JOB");
    return 1;
}

int redisSupervisedSystemd(void){
    const char *notify_socket = getenv("NOTIFY_SOCKET");
    int fd = 1;
    struct sockaddr_un su;
    struct iovec iov;
    struct msghdr hdr;
    int sendto_flags = 0;

    if(!notify_socket){
        serverLog(LL_WARNING,"systemd supervision requested, but NOTIFY_SOCKET not found"); 
        return 0; 
    };

    if((strchr("@/",notify_socket[0])) ==NULL || strlen(notify_socket) < 2){
        return 0; 
    };

    serverLog(LL_NOTICE,"supervised by systemd, will signal readiness");
    if((fd = socket(AF_UNIX,SOCK_DGRAM,0)) == -1){
        serverLog(LL_WARNING,"Can't connect to systemd socket %s,", notify_socket); 
        return 0;
    }

    memset(&su,0,sizeof(su));
    su.sun_family = AF_UNIX;
    strncpy(su.sun_path, notify_socket, sizeof(su.sun_path) - 1);
    su.sun_path[sizeof(su.sun_path) - 1] = '\0';
    
    if(notify_socket[0] == '@'){
        su.sun_path[0] = '\0'; 
    }; 


    memset(&iov, 0, sizeof(iov));
    iov.iov_base = "READY=1";
    iov.iov_len = strlen("READY=1");
   
    memset(&hdr, 0, sizeof(hdr)); 
    hdr.msg_name = &su;
    hdr.msg_namelen = offsetof(struct sockaddr_un, sun_path) + strlen(notify_socket);
    hdr.msg_iov = &iov;
    hdr.msg_iovlen = 1;

    unsetenv("NOTIFY_SOCKET");

#ifdef HAVE_MSG_NOSIGNAL
    sendto_flags |= MSG_NOSIGNAL;
#endif

    if(sendmsg(fd, &hdr, sendto_flags) < 0){
        serverLog(LL_WARNING, "Can't send notification to systemd"); 
        close(fd); 
        return 0;
    };
    
    close(fd);
    return 1;
};

int redisIsSupervised(int mode){
   if(mode == SUPERVISED_AUTODETECT){
        const char *upstart_job = getenv("UPSTART_JOB"); 
        const char *notify_socket = getenv("NOTIFY_SOCKET");

        if(upstart_job){
            redisSupervisedUpstart(); 
        }else if(notify_socket){
            redisSupervisedSystemd(); 
        }
    }else if(mode == SUPERVISED_UPSTART){
        return redisSupervisedUpstart(); 
    }else if(mode == SUPERVISED_SYSTEMD){
        return redisSupervisedSystemd(); 
    } 
    
   return 0;
};

int main(int argc, char **argv){
    struct timeval tv;
    int j;

#ifdef REDIS_TEST
    if(argc == 3 && !strcasecmp(argv[1], "test")){
        if(!strcasecmp(argv[2],"ziplist")){
            return ziplistTest(argc,argv); 
        }else if(!strcasecmp(argv[2],"quicklist")){
            quicklistTest(argc,argv); 
        }else if(!strcasecmp(argv[2],"intset")){
            return intsetTest(argc,argv); 
        }else if(!strcasecmp(argv[2],"zipmap")){
            return zipmapTest(argc,argv); 
        }else if(!strcasecmp(argv[2],"sha1test")){
            return sha1Test(argc,argv); 
        }else if(!strcasecmp(argv[2],"util")){
            return utilTest(argc,argv); 
        }else if(!strcasecmp(argv[2],"sds")){
            return sdsTest(argc,argv); 
        }else if(!strcasecmp(argv[2],"crc64")){
            return crc64Test(argc,argv); 
        };         

        return -1; 
    }
#endif

#ifdef INIT_SETPROCTITLE_REPLACEMENT
    spt_init(argc,argv);
#endif
    setlocale(LC_COLLATE,""); 
    zmalloc_enable_thread_safeness();
    zmalloc_set_oom_handler(redisOutOfMemoryHandler);
    srand(time(NULL)^getpid());
    gettimeofday(&tv,NULL);
    char hashseed[16];
    getRandomHexChars(hashseed,sizeof(hashseed));
    dictSetHashFunctionSeed((uint8_t*)hashseed);
    server.sentinel_mode = checkForSentinelMode(argc,argv);
    initServerConfig();
    moduleInitModulesSystem();

    server.executable = getAbsolutePath(argv[0]);
    server.exec_argv = zmalloc(sizeof(char*) *(argc+1));
    server.exec_argv[argc] = NULL;
    for(j = 0; j < argc; j++) server.exec_argv[j] = zstrdup(argv[j]);
    if(server.sentinel_mode){
        initSentinelConfig(); 
        initSentinel();
    }
    
    if(strstr(argv[0],"redis-check-rdb") != NULL){
        redis_check_rdb_main(argc,argv); 
    };

    if(argc >= 2){
        j = 1; 
        sds options = sdsempty();
        char *configfile = NULL;
        if(strcmp(argv[1],"-v") == 0 || strcmp(argv[1],"--version") == 0) version();
        if(strcmp(argv[1],"--help") == 0 || strcmp(argv[1],"-h") == 0) usage();
        if(strcmp(argv[1],"--test-memory") == 0){
            if(argc == 3){
                memtest(atoi(argv[2]),50); 
                exit(0);
            }else{
                fprintf(stderr,"Please specify the amount of memory to test in megabytes.\n"); 
                fprintf(stderr,"Example: ./redis-server --test-memory 4096\n\n" );
                exit(1);
            }; 
        };

        if(argv[j][0] != '-' || argv[j][1] != '-'){
            configfile = argv[j]; 
            server.configfile = getAbsolutePath(configfile);
            zfree(server.exec_argv[j]);
            server.exec_argv[j] = zstrdup(server.configfile);
            j++;
        };
        
        while(j != argc){
            if(argv[j][0] == '-' && argv[j][1] == '-'){
                if(!strcmp(argv[j],"--check-rdb")){
                    j++;
                    continue; 
                }; 

                if(sdslen(options)) options = sdscat(options,"\n");
                options = sdscat(options,argv[j]+2);
                options = sdscat(options," ");
            }else{
                options = sdscatrepr(options,argv[j],strlen(argv[j])); 
                options = sdscat(options," ");
            }; 

            j++;
        };

        if(server.sentinel_mode && configfile && *configfile == '-'){
            serverLog(LL_WARNING, "Sentinel config from STDIN not allowed."); 
            serverLog(LL_WARNING, "Sentinel needs config file on disk to save state. Exiting...");
            exit(1);
        }

        resetServerSaveParams();
        loadServerConfig(configfile,options);
        sdsfree(options);
    };
    

    serverLog(LL_WARNING,"oO0OoO0OoO0Oo Redis is starting oO0OoO0OoO0Oo");
    serverLog(LL_WARNING,"Redis version=%s, bits=%d, commit=%s, modified=%d,pid=%d,just started",
            REDIS_VERSION,
            (sizeof(long) == 8) ? 64 : 32,
            redisGitSHA1(),
            strtol(redisGitDirty(),NULL,10) > 0,
            (int)getpid());

    if(argc == 1){
        serverLog(LL_WARNING,"Warning: no config file specified, using the default config. In order to specify a config file use %s/path/to/%s.conf",argv[0],server.sentinel_mode ? "sentinel" : "redis"); 
    }else{
        serverLog(LL_WARNING,"Configuration loaded"); 
    }

    server.supervised = redisIsSupervised(server.supervised_mode);
    int background = server.daemonize && !server.supervised;
    if(background)  daemonize();
    initServer();
    if(background || server.pidfile) createPidFile();
    redisSetProcTitle(argv[0]); 
    redisAsciiArt();
    checkTcpBacklogSettings();
    
    if(!server.sentinel_mode){
        serverLog(LL_WARNING,"Server initialized"); 
    #ifdef __linux__
        linuxMemoryWarnings();
    #endif
        moduleLoadFromQueue();
        loadDataFromDisk();
        if(server.cluster_enabled){
            if(verifyClusterConfigWithData() == C_ERR){
                serverLog(LL_WARNING,"You can't have keys in a DB different than DB 0 when in Cluster mdoe. Exiting."); 
                exit(1);
            } 
        };

        if(server.ipfd_count > 0){
            serverLog(LL_NOTICE,"Ready to accept connections"); 
        }

        if(server.sofd > 0){
            serverLog(LL_NOTICE, "The server is now ready to accept connections at %s",server.unixsocket); 
        };
    }else{
        sentinelIsRunning(); 
    };

    if(server.maxmemory > 0 && server.maxmemory < 1024 * 1024){
        serverLog(LL_WARNING, "WARNING: You specified a maxmemory value that is less than 1MB (current value is %llu bytes). Are you sure this is what you really want?", server.maxmemory); 
    };

    aeSetBeforeSleepProc(server.el,beforeSleep);
    aeMain(server.el);
    aeDeleteEventLoop(server.el);
    return 0;
};

