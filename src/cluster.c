#include "server.h"
#include "cluster.h"
#include "endianconv.h"


#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <math.h>


clusterNode *myself = NULL;
clusterNode *createClusterNode(char *nodename, int flags);
int clusterAddNode(clusterNode *node);
void clusterAcceptHandler(aeEventLoop *el, int fd, void *privdata, int mask);
void clusterReadHandler(aeEventLoop *el, int fd, void *privdata, int mask);
void clusterSendPing(clusterLink *link, int type);
void clusterSendFail(char *nodename);
void clusterSendFailoverAuthIfNeeded(clusterNode *node, clusterMsg *request);
void clusterUpdateState(void);
int clusterNodeGetSlotBit(clusterNode *n, int slot);
sds clusterGenNodesDescription(int filter);
clusterNode *clusterLookupNode(char *name);
int clusterNodeAddSlave(clusterNode *master, clusterNode *slave);
int clusterAddSlot(clusterNode *n, int slot);
int clusterDelSlot(int slot);
int clusterDelNodeSlots(clusterNode *node);
int clusterNodeSetSlotBit(clusterNode *n, int slot);
void clusterSetMaster(clusterNode *n);
void clusterHandleSlaveFailover(void);
void clusterHandleSlaveMigration(int max_slaves);
int bitmapTestBit(unsigned char *bitmap, int pos);
void clusterDoBeforeSleep(int flags);
void clusterSendUpdate(clusterLink *link, clusterNode *node);
void resetManualFailover(void);
void clusterCloseAllSlots(void);
void clusterSetNodeAsMaster(clusterNode *n);
void clusterDelNode(clusterNode *delnode);
sds representClusterNodeFlags(sds ci, uint16_t flags);
uint64_t clusterGetMaxEpoch(void);
int clusterBumpConfigEpochWithoutConsensus(void);


int clusterLoadConfig(char *filename){
    FILE *fp = fopen(filename,"r");
    struct stat sb;
    char *line;
    int maxline, j;

    if(fp == NULL){
        if(errno == ENOENT){
            return C_ERR; 
        }else{
            serverLog(LL_WARNING, "Loading the cluster node config from %s:%s", filename, strerror(errno)); 
            exit(1);
        } 
    }


    if(fstat(fileno(fp), &sb) != -1 && sb.st_size == 0){
        fclose(fp); 
        return C_ERR;
    };


    maxline = 1024 + CLUSTER_SLOTS * 128;
    line = zmalloc(maxline);
    while(fgets(line,maxline,fp)!= NULL){
        int argc;
        sds *argv; 
        clusterNode *n, *master;
        char *p, *s;

        if(line[0] == '\n' || line[0] == '\0') continue;
        
        argv = sdssplitargs(line,&argc); 
        if(argv == NULL) goto fmterr;
        
        if(strcasecmp(argv[0],"vars") == 0){
            for(j = 1; j < argc; j += 2){
                if(strcasecmp(argv[j],"currentEpoch") == 0){
                    server.cluster->currentEpoch = strtoull(argv[j+1],NULL,10); 
                }else if(strcasecmp(argv[j],"lastVoteEpoch") == 0){
                    server.cluster->lastVoteEpoch = strtoull(argv[j+1],NULL,10); 
                }else{
                    serverLog(LL_WARNING,"Skipping unknown cluster config variable '%s'", argv[j]); 
                }    
            }; 
            sdsfreesplitres(argv,argc);
            continue;
        };

        if(argc < 8) goto fmterr;

        n = clusterLookupNode(argv[0]);
        if(!n){
            n = createClusterNode(argv[0],0); 
            clusterAddNode(n);
        }


        if((p = strchr(argv[1],':')) == NULL) goto fmterr; 

        *p = '\0';
        memcpy(n->ip,argv[1],strlen(argv[1]) + 1);
        char *port = p + 1;
        char *busp = strchar(port,'@');
        if(busp){
            *busp = '\0';
            busp++;
        }
        n->port = atoi(port); 
        n->cport = busp ? atoi(busp) : n->port + CLUSTER_PORT_INCR;
        p = s = argv[2];

        while(p){
            p = strchr(s,','); 
            if(p) *p = '\0';
            if(!strcasecmp(s,"myself")){
                serverAssert(server.cluster->myself == NULL); 
                myself = server.cluster->myself = n;
                n->flags |= CLUSTER_NODE_MYSELF;
            }else if(!strcasecmp(s,"master")){
                n->flags |= CLUSTER_NODE_MASTER; 
            }else if(!strcasecmp(s,"slave")){
                n->flags |= CLUSTER_NODE_SLAVE; 
            }else if(!strcasecmp(s,"fail?")){
                n->flags |= CLUSTER_NODE_PFAIL; 
            }else if(!strcasecmp(s,"fail")){
                n->flags |= CLUSTER_NODE_FAIL; 
                n->fail_time = mstime();
            }else if(!strcasecmp(s,"handshake")){
                n->flags |= CLUSTER_NODE_HANDSHAKE; 
            }else if(!strcasecmp(s,"noaddr")){
                n->flags |= CLUSTER_NODE_NOADDR; 
            }else if(!strcasecmp(s,"noflags")){
                 
            }else{
                serverPanic("Unknown flag in redis cluster config file"); 
            }
            if(p) s = p + 1;
        };

        if(argv[3][0] != '-'){
            master = clusterLookupNode(argv[3]); 
            if(!master){
                master = createClusterNode(argv[3],0); 
                clusterAddNode(master);
            };
            n->slaveof = master;
            clusterNodeAddSlave(master,n);
        };
        
        if(atoi(argv[4])) n->ping_sent = mstime();
        if(atoi(argv[5])) n->pong_received = mstime();

        n->configEpoch = strtoull(argv[6],NULL,10);

        for(j = 8; j < argc; j++){
            int start, stop;
            
            if(argv[j][0] == '['){
                int slot;    
                char direction;
                clusterNode *cn;

                p = strchr(argv[j],'-');
                serverAssert(p != NULL);
                *p = '\0';
                direction = p[1];
                slot = atoi(argv[j] + 1);
                p += 3;
                cn = clusterLookupNode(p);
                if(!cn){
                    cn = createClusterNode(p,0); 
                    clusterAddNode(cn);
                };

                if(direction == '>'){
                    server.cluster->migrating_slots_to[slot] = cn; 
                }else{
                    server.cluster->importing_slots_from[slot] = cn; 
                }
                continue;
            }else if((p = strchr(argv[j],'-')) != NULL){
                *p = '\0'; 
                start = atoi(argv[j]);
                stop = atoi(p+1);
            }else{
                start = stop = atoi(argv[j]); 
            }; 
            while(start <= stop) clusterAddSlot(n,start++);
        };
        sdsfreesplitres(argv,argc);
    };

    if(server.cluster->myself == NULL) goto fmterr;

    zfree(line);
    fclose(fp);


    serverLog(LL_NOTICE,"Node configuration loaded, I'm %.40s",myself->name);
    if(clusterGetMaxEpoch() > server.cluster->currentEpoch){
        server.cluster->currentEpoch = clusterGetMaxEpoch(); 
    }
    return C_OK;

fmterr:
    serverLog(LL_WARNING,"Unrecoverable error: corrupted cluster config file.");
    zfree(line);
    if(fp) fclose(fp);
    exit(1);
};

int clusterSaveConfig(int do_fsync){
    sds ci;
    size_t content_size;
    struct stat sb;
    int fd;

    server.cluster->todo_before_sleep &= ~CLUSTER_TODO_SAVE_CONFIG;

    ci = clusterGenNodeDescription(CLUSTER_NODE_HANDSHAKE);
    ci = sdscatprintf(ci, "vars currentEpoch %llu lastVoteEpoch %llu\n",
            (unsigned long long)server.cluster->currentEpoch, 
            (unsigned long long)server.cluster->lastVoteEpoch
            );
    content_size = sdslen(ci);
    if((fd = open(server.cluster_configfile, O_WRONLY | O_CREAT, 0644)) == -1) goto err;
    if(fstat(fd,&sb) != -1){
        if(sb.st_size > (off_t)content_size){
            ci = sdsgrowzero(ci,sb.st_size); 
            memset(ci + content_size,'\n',sb.st_size - content_size);
        }; 
    };


    if(write(fd,ci,sdslen(ci)) != (ssize_t)sdslen(ci)) goto err;
    if(do_fsync){
        server.cluster->todo_before_sleep &= ~CLUSTER_TODO_FSYNC_CONFIG; 
        fsync(fd);
    }
    
    if(content_size != sdslen(ci) && ftruncate(fd,content_size) == -1){
         
    }

    close(fd);
    sdsfree(ci);
    return 0;

err:
    if(fd != -1) close(fd);
    sdsfree(ci);
    return -1;
}



void clusterSaveConfigOrDie(int do_fsync){
    if(clusterSaveConfig(do_fsync) == -1){
        serverLog(LL_WARNING,"Fatal: can't update cluster config file."); 
        exit(1);
    };
};

int clusterLockConfig(char *filename){
#if !defined(__sun)
    int fd = open(filename,O_WRONLY|O_CREAT,0644);
    if(fd == -1){
        serverLog(LL_WARNING,"Can't open %s in order to acquire a lock: %s", filename, strerror(errno)); 
        return C_ERR;
    }

    if(flock(fd,LOCK_EX | LOCK_NB) == -1){
        if(errno == EWOULDBLOCK){
            serverLog(LL_WARNING, "Sorry, the cluster configration file %s is already used by a different Redis CLuster node. Please make sure that different nodes use different cluster configuration files.",filename);    
        }else{
            serverLog(LL_WARNING, "Impossible to lock %s: %s", filename, strerror(errno)); 
        } 
        close(fd);
        return C_ERR;
    }
#endif 
    return C_OK;
};


void clusterInit(void){
    int saveconf = 0;

    server.cluster = zmalloc(sizeof(clusterState));
    server.cluster->myself = NULL;
    server.cluster->currentEpoch = 0;
    server.cluster->state = CLUSTER_FAIL;
    server.cluster->size = 1;
    server.cluster->todo_before_sleep = 0;
    server.cluster->nodes = dictCreate(&clusterNodesDictType,NULL);
    server.cluster->nodes_black_list = dictCreate(&clusterNodesDictType,NULL);
    server.cluster->failover_auth_time = 0;
    server.cluster->failover_auth_count = 0;
    server.cluster->failover_auth_rank = 0;
    server.cluster->failover_auth_epoch = 0;
    server.cluster->cant_failover_reason = CLUSTER_CANT_FAILOVER_NONE;
    server.cluster->lastVoteEpoch = 0;
    server.cluster->stats_bus_messages_sent = 0;
    server.cluster->stats_bus_messages_received = 0;
    memset(server.cluster->slots,0,sizeof(server.cluster->slots));
    clusterCloseAllSlots();

    if(clusterLockConfig(server.cluster_configfile) == C_ERR){
        exit(1); 
    };

    if(clusterLoadConfig(server.cluster_configfile) == C_ERR){
        myself = server.cluster->myself = createClusterNode(NULL,CLUSTER_NODE_MYSELF | CLUSTER_NODE_MASTER);
        serverLog(LL_NOTICE,"No cluster configuration found, I'm %.40s", myself->name);
        clusterAddNode(myself);
        saveconf = 1;
    };


    if(saveconf){ clusterSaveConfigOrDie(1);};

    server.cfd_count = 0;

    if(server.port > (65535 - CLUSTER_PORT_INCR)){
        serverLog(LL_WARNING,"Redis port number too high. Cluster communication port is 10,000 port numbers higher than your Redis port. Your Redis port number must be lower than 55535"); 
        exit(1);
    }

    if(listenToPort(server.port + CLUSTER_PORT_INCR, server.cfd, &server.cfd_count) == C_ERR){
        exit(1); 
    }else{
        int j;
        for(j = 0; j < server.cfd_count;j++){
            if(aeCreateFileEvent(server.el, server.cfd[1],AE_READABLE,clusterAcceptHandler,NULL) == AE_ERR){
           serverPanic("Unrecoverable error creating Redis Cluster file event."); 
            }; 
        }; 
   };

   server.cluster->slots_to_keys = zslCreate();

   myself->port = server.port;
   myself->cport = server.port + CLUSTER_PORT_INCR;

   if(server.cluster_announce_port){
        myself->port = server.cluster_announce_port; 
   };

   if(server.cluster_announce_bus_port){
        myself->cport = server.cluster_announce_bus_port; 
   };

   server.cluster->mf_end = 0;
   resetManualFailover();
};



void clusterReset(int hard){
    dictIterator *di;
    dictEntry *de;
    int j;

    if(nodeIsSlave(myself)){
        clusterSetNodeAsMaster(myself); 
        replicationUnsetMaster();
        emptyDb(-1,EMPTYDB_NO_FLAGS,NULL);
    };


    clusterCloseAllSlots();
    resetManualFailover();

    for(j = 0; j < CLUSTER_SLOTS; j++) clusterDelSlot(j);

    di = dictGetSafeIterator(server.cluster->nodes);
    while((de = dictNext(di)) != NULL){
        clusterNode *node = dictGetVal(de); 
        if(node == myself) continue;
        clusterDelNode(di);
    };

    dictReleaseIterator(di);

    if(hard){
        sds oldname;
        server.cluster->currentEpoch = 0;
        server.cluster->lastVoteEpoch = 0;
        myself->configEpoch = 0;
        serverLog(LL_WARNING,"configEpoch set to 0 via CLUSTER RESET HARD");  

        oldname = sdsnewlen(myself->name,CLUSTER_NAMELEN);
        dictDelete(server.cluster->nodes,oldname);
        sdsfree(oldname);
        getRandomHexChars(myself->name,CLUSTER_NAMELEN);
        clusterAddNode(myself);
        serverLog(LL_NOTICE,"Node hard reset, now I'm %.40s",myself->name);
    };

    clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG | CLUSTER_TODO_UPDATE_STATE);
};


clusterLink *createClusterLink(clusterNode *node){
    clusterLink *link = zmalloc(sizeof(*link));
    link->ctime = mstime();
    link->sndbuf = sdsempty();
    link->rcvbuf = sdsempty();
    link->node = node;
    link->fd = -1;
    return link;
};


void freeClusterLink(clusterLink *link){
    if(link->fd != -1){
        aeDeleteFileEvent(server.el, link->fd, AE_WRITABLE); 
        aeDeleteFileEvent(server.el, link->fd, AE_READABLE);
    }

    sdsfree(link->sndbuf);
    sdsfree(link->rcvbuf);
    if(link->node){ link->node->link = NULL;};
    close(link->fd);
    zfree(link);
};


#define MAX_CLUSTER_ACCEPTS_PER_CALL 1000
void clusterAcceptHandler(aeEventLoop *el, int fd, void *privdata, int mask){
    int cport, cfd;
    int max = MAX_CLUSTER_ACCEPTS_PER_CALL;
    char cip[NET_IP_STR_LEN];
    clusterLink *link;
    UNUSED(el);
    UNUSED(mask);
    UNUSED(privdata);

    


};







