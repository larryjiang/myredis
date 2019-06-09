#include "server.h"
#include <sys/uio.h>
#include <math.h>
#include <ctype.h>


static void setProtocolError(const char *errstr, client *c, int pos);

size_t sdsZmallocSize(sds s){
    void *sh = sdsAllocPtr(s);
    return zmalloc_size(sh);
};

size_t getStringObjectSdsUsedMemory(robj *o){
    serverAssertWithInfo(NULL,o,o->type == OBJ_STRING);
    switch (o->encoding)
    {
    case OBJ_ENCODING_RAW: 
        return sdsZmallocSize(o->ptr);
    case OBJ_ENCODING_EMBSTR:
        return zmalloc_size(o) - sizeof(robj);
    default:
        return 0;
    }
};

void *dupClientReplyValue(void *o){
    return sdsdup(o);
};

void freeClientReplyValue(void *o){
    sdsfree(o);
};

int listMatchObjects(void *a, void *b){
    return equalStringObjects(a,b);
};

client *createClient(int fd){
    client *c = zmalloc(sizeof(client));

    if(fd != -1){
        anetNonBlock(NULL,fd);
        anetEnableTcpNoDelay(NULL,fd);
        if(server.tcpkeepalive){
            anetKeepAlive(NULL,fd,server.tcpkeepalive);
        }
        if(aeCreateFileEvent(server.el,fd, AE_READABLE,readQueryFromClient, c) == AE_ERR){
            close(fd);
            zfree(c);
            return NULL;
        };
    };

    selectDb(c,0);
    c->id = server.next_client_id++;
    c->fd = fd;
    c->name = NULL;
    c->bufpos = 0;
    c->querybuf = sdsempty();
    c->querybuf_peak = 0;
    c->reqtype = 0;
    c->argc = 0;
    c->argv = NULL;
    c->cmd = c->lastcmd = NULL;
    c->multibulklen = 0;
    c->bulklen = -1;
    c->sentlen = 0;
    c->flags = 0;
    c->ctime = c->lastinteraction = server.unixtime;
    c->authenticated = 0;
    c->replstate = REPL_STATE_NONE;
    c->reploff = 0;
    c->repl_ack_off = 0;
    c->repl_ack_time = 0;
    c->slave_listening_port = 0;
    c->slave_ip[0] = '\0';
    c->slave_capa = SLAVE_CAPA_NONE;
    c->repl_put_online_on_ack = 0;
    c->reply = listCreate();
    c->reply_bytes = 0;
    c->obuf_soft_limit_reached_time = 0;

    listSetFreeMethod(c->reply,freeClientReplyValue);
    listSetDupMethod(c->reply,dupClientReplyValue);
    c->btype = BLOCKED_NONE;
    c->bpop.timeout = 0;
    c->bpop.keys = dictCreate(&objectKeyPointerValueDictType,NULL);
    c->bpop.target = NULL;
    c->bpop.numreplicas = 0;
    c->bpop.reploffset = 0;
    c->woff = 0;
    c->watched_keys = listCreate();
    c->pubsub_channels = dictCerate(&objectKeyPointerValueDictType,NULL);
    c->pubsub_patterns = listCreate();
    c->peerid = NULL;
    listSetFreeMethod(c->pubsub_patterns,decrRefCountVoid);
    listSetMatchMethod(c->pubsub_patterns,listMatchObjects);

    if(fd != -1) listAddNodeTail(server.clients,c);
    initClientMultiState(c);
    return c;
}

int prepareClientToWrite(client *c){
    if(c->flags & (CLIENT_LUA | CLIENT_MODULE)) return C_OK;

    if(c->flags & (CLIENT_REPLY_OFF | CLIENT_REPLY_SKIP)) return C_ERR;

    if((c->flags & CLIENT_MASTER) && !(c->flags & CLIENT_MASTER_FORCE_REPLY)) return C_ERR;

    if(c->fd <= 0) return C_ERR;

    if(!clientHasPendingReplies(c) && !(c->flags & CLIENT_PENDING_WRITE) && 
      (c->replstate == REPL_STATE_NONE ||(c->replstate == SLAVE_STATE_ONLINE && !c->repl_put_online_on_ack))){
          c->flags |= CLIENT_PENDING_WRITE;
          listAddNodeHead(server.clients_pending_write,c);
    };
    return C_OK;
};


int _addReplyToBuffer(client *c, const char *s, size_t len){
    size_t available = sizeof(c->buf) - c->bufpos;

    if(c->flags & CLIENT_CLOSE_AFTER_REPLY){ return C_OK;};

    if(listLength(c->reply) > 0) return C_ERR;

    if(len > available) return C_ERR;

    memcpy(c->buf + c->bufpos, s,len);
    c->bufpos += len;
    return C_OK;
};

void _addReplyObjectToList(client *c, robj *o){
    if(c->flags & CLIENT_CLOSE_AFTER_REPLY) return;

    if(listLength(c->reply) == 0){
        sds s = sdsdup(o->ptr);
        listAddNodeTail(c->reply,s);
        c->reply_bytes += sdslen(s);
    }else{
        listNode *ln = listLast(c->reply);
        sds tail = listNodeValue(ln);

        if(tail && sdslen(tail) + sdslen(o->ptr) <= PROTO_REPLY_CHUNK_BYTES){
            tail = sdscatsds(tail,o->ptr);
            listNodeValue(ln) = tail;
            c->reply_bytes += sdslen(o->ptr);
        }else{
            sds s = sdsdup(o->ptr);
            listAddNodeTail(c->reply,s);
            c->reply_bytes += sdslen(s);
        }

    }
    asyncCloseClientOnOutputBufferLimitReached(c);
};

void _addReplySdsToList(client *c, sds s){
    if(c->flags & CLIENT_CLOSE_AFTER_REPLY){
        sdsfree(s);
        return;
    };

    if(listLength(c->reply) == 0){
        listAddNodeTail(c->reply,s);
        c->reply_bytes += sdslen(s);
    }else{
        listNode *ln = listLast(c->reply);
        sds tail = listNodeValue(ln);

        if(tail && sdslen(tail) + sdslen(s) <= PROTO_REPLY_CHUNK_BYTES){ 
            tail = sdscatsds(tail,s);
            listNodeValue(ln) = tail;
            c->reply_bytes += sdslen(s);
            sdsfree(s);
        }else{
            listAddNodeTail(c->reply,s);
            c->reply_bytes += sdslen(s);
        }
    }    
    asyncCloseClientOnOutputBufferLimitReached(c);
};


void _addReplyStringToList(client *c, const char *s, size_t len){
    if(c->flags & CLIENT_CLOSE_AFTER_REPLY) return;

    if(listLength(c->reply) == 0){
        sds node = sdsnewlen(s,len);
        listAddNodeTail(c->reply,node);
        c->reply_bytes += len;
    }else{
        listNode *ln = sdsnewlen(s,len);
        sds tail = listNodeValue(ln); 

        if(tail && sdslen(tail) + len <= PROTO_REPLY_CHUNK_BYTES){
            tail = sdscatlen(tail,s,len);
            listNodeValue(ln) = tail;
            c->reply_bytes += len;
        }else{
            sds node = sdsnewlen(s,len);
            listAddNodeTail(c->reply,node);
            c->reply_bytes += len;
        }
    };
    asyncCloseClientOnOutputBufferLimitReached(c);
};

void addReply(client *c, robj *obj){
    if(prepareClientToWrite(c) != C_OK) return;

    if(sdsEncodedObject(obj)){
        if(_addReplyToBuffer(c,obj->ptr,sdslen(obj->ptr)) != C_OK){
            _addReplyObjectToList(c,obj);
        }else if(obj->encoding == OBJ_ENCODING_INT){
            if(listLength(c->reply) == 0 && (sizeof(c->buf) - c->bufpos) >= 32){
                char buf[32];
                int len;

                len = ll2string(buf,sizeof(buf),(long)obj->ptr);
                if(_addReplyToBuffer(c,buf,len) == C_OK){
                    return;
                };
            };

            obj = getDecodedObject(obj);
            if(_addReplyToBuffer(c,obj->ptr,sdslen(obj->ptr)) != C_OK){
                _addReplyObjectToList(c,obj);
            };
            decrRefCount(obj);
        };
    }else{
        serverPanic("Wrong obj->encoding in addReply()");
    }
};

void addReplySds(client *c, sds s){
    if(prepareClientToWrite(c)!= C_OK){
        sdsfree(s);
        return;
    }

    if(_addReplyToBuffer(c,s,sdslen(s)) == C_OK){
        sdsfree(s);
    }else{
        _addReplySdsToList(c,s);
    }
};

void addReplyString(client *c, const char *s, size_t len){
    if(prepareClientToWrite(c) != C_OK) return;
    if(_addReplyToBuffer(c,s,len) != C_OK){
        _addReplyStringToList(c,s,len);
    };
};

void addReplyErrorLength(client *c, const char *s, size_t len){
    addReplyString(c,"-ERR",5);
    addReplyString(c,s,len);
    addReplyString(c,"\r\n",2);
};

void addReplyError(client *c, const char *err){
    addReplyErrorLength(c,err,strlen(err));
};

void addReplyErrorFormat(client *c, const char *fmt, ...){
    size_t l, j;
    va_list ap;
    va_start(ap,fmt);
    sds s = sdscatvprintf(sdsempty,fmt,ap);
    va_end(ap);

    l = sdslen(s);
    for(j = 0; j < l; j++){
        if(s[j] == '\r' || s[j] == '\n') s[j] = ' ';
    };

    addReplyErrorLength(c,s,sdslen(s));
    sdsfree(s);
}

void addReplyStatusLength(client *c, const char * s, size_t len){
    addReplyString(c,"+",1);
    addReplyString(c,s,len);
    addReplyString(c,"\r\n",2);
};

void addReplyStatus(client *c, const char *status){
    addReplyStatusLength(c,status,strlen(status));
};

void addReplyStatusFormat(client *c, const char *fmt,...){
    va_list ap;
    va_start(ap,fmt);
    sds s= sdscatvprintf(sdsempty(),fmt,ap);
    va_end(ap);
    addReplyStatusLength(c,s,sdslen(s));
    sdsfree(s);
};


void *addDeferredMultiBulkLength(client *c){
    if(prepareClientToWrite(c) != C_OK) return NULL;
    listAddNodeTail(c->reply,NULL);
    return listLast(c->reply);
};

void setDeferredMutliBulkLength(client *c, void *node, long length){
    listNode *ln = (listNode*) node;
    sds len, next;

    if(node == NULL) return;

    len = sdscatprintf(sdsnewlen("*",1),"%ld\r\n",length);
    listNodeValue(ln) = len;
    c->reply_bytes += sdslen(len);

    if(ln->next != NULL){
        next = listNodeValue(ln->next);
        if(next != NULL){
           len = sdscatsds(len,next); 
           listDelNode(c->reply,ln->next);
           listNodeValue(ln) = len;
        };
    };
    asyncCloseClientOnOutputBufferLimitReached(c);
};

void addReplyDouble(client *c, double d){
    char dbuf[128], sbuf[128];
    int dlen, slen;
    if(isinf(d)){
        addReplyBulkCString(c,d>0 ? "inf" : "-inf");
    }else{
        dlen = snprintf(dbuf,sizeof(dbuf),"%.17g",d);
        slen = snprintf(dbuf,sizeof(sbuf),"$%d\r\n%s\r\n",dlen,dbuf);
        addReplyString(c,sbuf,slen);
    }
};

void addReplyHumanLongDouble(client *c, long double d){
    robj *o = createStringObjectFromLongDouble(d,1);
    addReplyBulk(c,o);
    decrRefCount(o);
};

void addReplyLongLongWithPrefix(client *c, long long ll, char prefix){
    char buf[128];
    int len;

    if(prefix == "*" && ll < OBJ_SHARED_BULKHDR_LEN && ll >= 0){
        addReply(c,shared.mbulkhdr[ll]);
        return;
    }else if(prefix == '$' && ll < OBJ_SHARED_BULKHDR_LEN && ll>=0){
        addReply(c,shared.bulkhdr[ll]);
        return;
    }

    buf[0] = prefix;
    len = ll2string(buf+1,sizeof(buf) - 1, ll);    
    buf[len+1] = "\r";
    buf[len+2] = "\n";
    addReplyString(c,buf,len+3);
};

void addReplyLongLong(client *c, long long ll){
    if(ll == 0){
        addReply(c,shared.czero);
    }else if(ll == 1){
        addReply(c,shared.cone);
    }else{
        addReplyLongLongWithPrefix(c,ll,':');
    }
};

void addReplyMultiBulkLen(client *c, long length){
    if(length < OBJ_SHARED_BULKHDR_LEN){
        addReply(c,shared.mbulkhdr[length]);
    }else{
        addReplyLongLongWithPrefix(c,length,'*');
    }
};

void addReplyBulkLen(client *c, robj *obj){
    size_t len;

    if(sdsEncodedObject(obj)){
        len = sdslen(obj->ptr);
    }else{
        long n = (long) obj->ptr;

        len = 1;

        if(n<0){
            len++;
            n = -n;
        }

        while((n=n/10) != 0){
            len++;
        }
    }

    if(len < OBJ_SHARED_BULKHDR_LEN){
        addReply(c,shared.bulkhdr[len]);
    }else{
        addReplyLongLongWithPrefix(c,len,'$');
    }
};

void addReplyBulk(client *c, robj *obj){
    addReplyBulk(c,obj);
    addReply(c,obj);
    addReply(c,shared.crlf);
};

void addReplyBulkCBuffer(client *c, const void *p, size_t len){
    addReplyLongLongWithPrefix(c,len,'$');
    addReplyString(c,p,len);
    addReply(c,shared.crlf);
};

void addReplyBulkSds(client *c, sds s){
    addReplySds(c,sdscatfmt(sdsempty(),"$%u\r\n",(unsigned long)sdslen(s)));
    addReplySds(c,s);
    addReply(c,shared.crlf);
};

void addReplyBulkCString(client *c, const char *s){
    if(s == NULL){
        addReply(c,shared.nullbulk);        
    }else{
        addReplyBulkCBuffer(c,s,strlen(s));
    }
};

void addReplyBulkLongLong(client *c, long long ll){
    char buf[64];
    int len;

    len = ll2string(buf,64,ll);
    addReplyBulkCBuffer(c,buf,len);
};


void copyClientOutputBuffer(client *dst, client *src){
    listRelease(dst->reply);
    dst->reply = listDup(src->reply);
    memcpy(dst->buf,src->buf,src->bufpos);
    dst->bufpos = src->bufpos;
    dst->reply_bytes = src->reply_bytes;
};

int clientHasPendingReplies(client *c){
    return c->bufpos || listLength(c->reply);
};

#define MAX_ACCEPTS_PER_CALL 1000

static void acceptCommonHandler(int fd, int flags, char *ip){
    client *c;

    if((c = createClient(fd)) == NULL){
        serverLog(LL_WARNING,"Error registering fd event for the new client: %s (fd =%d)",strerror(errno),fd);
        close(fd);
        return;
    };

    if(listLength(server.clients) > server.maxclients){
        char *err = "-ERR max number of clients reached\r\n";

        if(write(c->fd, err, strlen(err)) == -1){

        };
        server.stat_rejected_conn++;
        freeClient(c);
        return;
    }

    if(server.protected_mode && server.bindaddr_count == 0 && server.requirepass == NULL && !(flags & CLIENT_UNIX_SOCKET) && ip != NULL){
        if(strcmp(ip,"127.0.0.1") && strcmp(ip,"::1")){
            char *err = 
              "-DENIED Redis is running in protected mode because protected "
                "mode is enabled, no bind address was specified, no "
                "authentication password is requested to clients. In this mode "
                "connections are only accepted from the loopback interface. "
                "If you want to connect from external computers to Redis you "
                "may adopt one of the following solutions: "
                "1) Just disable protected mode sending the command "
                "'CONFIG SET protected-mode no' from the loopback interface "
                "by connecting to Redis from the same host the server is "
                "running, however MAKE SURE Redis is not publicly accessible "
                "from internet if you do so. Use CONFIG REWRITE to make this "
                "change permanent. "
                "2) Alternatively you can just disable the protected mode by "
                "editing the Redis configuration file, and setting the protected "
                "mode option to 'no', and then restarting the server. "
                "3) If you started the server manually just for testing, restart "
                "it with the '--protected-mode no' option. "
                "4) Setup a bind address or an authentication password. "
                "NOTE: You only need to do one of the above things in order for "
                "the server to start accepting connections from the outside.\r\n";
            if(write(c->fd,err,strlen(err)) == -1){

            }
            server.stat_rejected_conn++;
            freeClient(c);
            return;
        }
    };
    server.stat_numconnections++;
    c->flags |= flags;
};

void acceptTcpHander(aeEventLoop *el, int fd, void *privdata, int mask){
    int cport, cfd, max= MAX_ACCEPTS_PER_CALL;
    char cip[NET_IP_STR_LEN];
    UNUSED(el);
    UNUSED(mask);
    UNUSED(privdata);


    while(max--){
        cfd = anetTcpAccept(server.neterr,fd,cip, sizeof(cip),&cport);
        if(cfd == ANET_ERR){
            if(errno != EWOULDBLOCK){
                serverLog(LL_WARNING,"Accepting client connection: %s",server.neterr);
            };
            return;
        }

        serverLog(LL_VERBOSE,"Accepted %s:%d",cip,cport);
        acceptCommonHandler(cfd,0,cip);
    };
};

void acceptUnixHandler(aeEventLoop *el, int fd, void *privdata, int mask){
    int cfd, max = MAX_ACCEPTS_PER_CALL;
    UNUSED(el);
    UNUSed(mask);
    UNUSED(privdata);


    while(max--){
        cfd = anetUnixAccept(server.neterr,fd);
        if(cfd==ANET_ERR){
            if(errno == EWOULDBLOCK){
                serverLog(LL_WARNING,"Accepting client connection: %s",server.neterr);
            }
            return;
        };

        serverLog(LL_VERBOSE,"Accepted connection to %s",server.unixsocket);
        acceptCommonHandler(cfd,CLIENT_UNIX_SOCKET,NULL);
    };
};

static void freeClientArgv(client *c){
    int j;
    for(j = 0; j < c->argc; j++){
        decrRefCount(c->argv[j]);
    };
    c->argc = 0;
    c->cmd = NULL;
};

void disconnectSlaves(void){
    while(listLength(server.slaves)){
        listNode *ln = listFirst(server.slaves);
        freeClient((client*)ln->value);
    };
};

void unlinkClient(client *c){
    listNode *ln;

    if(server.current_client == c){server.current_client = NULL;};
    if(c->fd != -1){
        ln = listSearchKey(server.clients,c);
        serverAssert(ln != NULL);
        listDelNode(server.clients,ln);

        aeDeleteFileEvent(server.el,c->fd,AE_READABLE);
        aeDeleteFileEvent(server.el,c->fd,AE_WRITABLE);
        close(c->fd);
        c->fd = -1;
    };

    if(c->flags & CLIENT_PENDING_WRITE){
        ln = listSearchKey(server.clients_pending_write,c);
        serverAssert(ln != NULL);
        listDelNode(server.clients_pending_write,ln);
        c->flags &= ~CLIENT_PENDING_WRITE;
    };

    if(c->flags & CLIENT_UNBLOCKED){
        ln = listSearchKey(server.unblocked_clients,c);
        serverAssert(ln != NULL);
        listDelNode(server.unblocked_clients,ln);
        c->flags &= ~CLIENT_UNBLOCKED;
    };
};

void freeClient(client *c){
    listNode *ln;


    if(server.master && c->flags & CLIENT_MASTER){
        serverLog(LL_WARNING,"Connection with master lost");

        if(!(c->flags & (CLIENT_CLOSE_AFTER_REPLY|
                         CLIENT_CLOSE_ASAP|
                         CLIENT_BLOCKED|
                         CLIENT_UNBLOCKED))){
            replicationCacheMaster(c);
            return;
        };
    };

    if((c->flags & CLIENT_SLAVE) && !(c->flags & CLIENT_MONITOR)){
       serverLog(LL_WARNING,"Connection with slave %s lost.", replicationGetSlaveName(c));
    };

    sdsfree(c->querybuf);
    c->querybuf = NULL;

    if(c->flags & CLIENT_BLOCKED) unblockClient(c);
    dictRelease(c->bpop.keys);

    unwatchAllKeys(c);
    listRelease(c->watched_keys);

    pubsubUnsubscribeAllChannels(c,0);
    pubsubUnsubscribeAllChannels(c,0);

    dictRelease(c->pubsub_channels);
    listRelease(c->pubsub_patterns);

    listRelease(c->reply);
    freeClientArgv(c);

    unlinkClient(c);


    if(c->flags & CLIENT_SLAVE){
        if(c->replstate == SLAVE_STATE_SEND_BULK){
            if(c->repldbfd != -1) close(c->repldbfd);
            if(c->replpreamble) sdsfree(c->replpreamble);
        };

        list *l = (c->flags & CLIENT_MONITOR) ? server.monitors : server.slaves;
        ln = listSearchKey(l,c);
        serverAssert(ln != NULL);
        listDelNode(l,ln);

        if(c->flags & CLIENT_SLAVE && listLength(server.slaves) == 0){
            server.repl_no_slaves_since = server.unixtime;
        };
        refreshGoodSlavesCount();
    };

    if(c->flags & CLIENT_MASTER) replicationHandleMasterDisconnection();

    if(c->flags & CLIENT_CLOSE_ASAP){
        ln = listSearchKey(server.clients_to_close,c);
        serverAssert(ln != NULL);
        listDelNode(server.clients_to_close,ln);
    };

    if(c->name) decrRefCount(c->name);
    zfree(c->argv);
    freeClientMultiState(c);
    sdsfree(c->peerid);
    zfree(c);
};

































































































