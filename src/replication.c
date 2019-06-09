#include "server.h"
#include <sys/time.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/stat.h>


void replicationDiscardCachedMaster(void);
void replicationResurrectCachedMaster(int newfd);
void replicationSendAck(void);
void putSlaveOnline(client *slave);
int cancelReplicationHandshake(void);

char *replicationGetSlaveName(client *c){
    static char buf[NET_PEER_ID_LEN];
    char ip[NET_IP_STR_LEN];

    ip[0] = '\0';
    buf[0] = '\0';


    if(c->slave_ip[0] != '\0' || anetPeerToString(c->fd,ip,sizeof(ip),NULL) != -1){
        if(c->slave_ip[0] != '\0') memcpy(ip,c->slave_ip, sizeof(c->slave_ip));

        if(c->slave_listening_port){
            anetFormatAddr(buf,sizeof(buf),ip,c->slave_listening_port);
        }else{
            snprintf(buf,sizeof(buf),"%s:<unknown-slave-port>",ip);
        };
    }else{
        snprintf(buf,sizeof(buf),"client id #%llu",(unsigned long long)c->id);
    };
    return buf;
}

void createReplicationBacklog(void){
    serverAssert(server.repl_backlog == NULL);
    server.repl_backlog = zmalloc(server.repl_backlog_size);
    server.repl_backlog_histlen = 0;
    server.repl_backlog_idx = 0;

    server.repl_backlog_off = server.master_repl_offset + 1;
};

void resizeReplicationBacklog(long long newsize){
    if(newsize < CONFIG_REPL_BACKLOG_MIN_SIZE){
        newsize = CONFIG_REPL_BACKLOG_MIN_SIZE;
    };

    server.repl_backlog_size = newsize;
    if(server.repl_backlog != NULL){
        zfree(server.repl_backlog);
        server.repl_backlog = zmalloc(server.repl_backlog_size);
        server.repl_backlog_histlen = 0;
        server.repl_backlog_idx = 0;
        server.repl_backlog_off = server.master_repl_offset;
    };
};

void freeReplicationBacklog(void){
    serverAssert(listLength(server.slaves));
    zfree(server.repl_backlog);
    server.repl_backlog = NULL;
}

void feedReplicationBacklog(void *ptr, size_t len){
    unsigned char *p = ptr;

    server.master_repl_offset += len;

    while(len){
        size_t thislen = server.repl_backlog_size - server.repl_backlog_idx;

        if(thislen > len){
            thislen = len;
        };

        memcpy(server.repl_backlog + server.repl_backlog_idx, p, thislen);
        server.repl_backlog_idx += thislen;
        if(server.repl_backlog_idx == server.repl_backlog_size){
            server.repl_backlog_idx = 0;
        };
        len -= thislen;
        p += thislen;
        server.repl_backlog_histlen  += thislen;
    };

    if(server.repl_backlog_histlen > server.repl_backlog_size){
        server.repl_backlog_histlen = server.repl_backlog_size;
    };

    server.repl_backlog_off = server.master_repl_offset - server.repl_backlog_histlen + 1;
};

void feedReplicationBacklogWithObject(robj *o){
    char llstr[LONG_STR_SIZE];
    void *p;
    size_t len;

    if(o->encoding == OBJ_ENCODING_INT){
        len = ll2string(llstr, sizeof(llstr),(long)o->ptr);
        p = llstr;
    }else{
        len = sdslen(o->ptr);
        p = o->ptr;
    }

    feedReplicationBacklog(p,len);
};



void unblockClientWaitingReplicas(client *c){
    listNode *ln = listSearchKey(server.clients_waiting_acks,c);
    serverAssert(ln != NULL);
    listDelNode(server.clients_waiting_acks,ln);
};





















