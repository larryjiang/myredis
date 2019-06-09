#ifndef __CLUSTER_H
#define __CLUSTER_H



#define CLUSTER_SLOTS 16384
#define CLUSTER_OK 0
#define CLUSTER_FAIL 1
#define CLUSTER_NAMELEN 40
#define CLUSTER_PORT_INCR 10000



#define CLUSTER_DEFAULT_NODE_TIMEOUT 15000
#define CLUSTER_DEFAULT_SLAVE_VALIDITY 10
#define CLUSTER_DEFAULT_REQUIRE_FULL_COVERAGE 1
#define CLUSTER_FAIL_REPORT_VALIDITY_MULT 2
#define CLUSTER_FAIL_UNDO_TIME_MULT 2
#define CLUSTER_FAIL_UNDO_TIME_ADD 10 
#define CLUSTER_FAILOVER_DELAY 5
#define CLUSTER_DEFAULT_MIGRATION_BARRIER 1
#define CLUSTER_MF_TIMEOUT 5000
#define CLUSTER_MF_PAUSE_MULT 2 
#define CLUSTER_SLAVE_MIGRATION_DELAY 5000



#define CLUSTER_REDIR_NONE 0
#define CLUSTER_REDIR_CROSS_SLOT 1
#define CLUSTER_REDIR_UNSTABLE #define CLUSTER_REDIR_ASK 3
#define CLUSTER_REDIR_MOVED 4
#define CLUSTER_REDIR_DOWN_STATE 5
#define CLUSTER_REDIR_DOWN_UNBOUND 6


struct clusterNode;

typedef struct clusterLink{
    mstime_t ctime;
    int fd;
    sds sndbuf;
    sds rcvbuf;
    struct clusterNode *node;
} clusterLink;


#define CLUSTER_NODE_MASTER 1
#define CLUSTER_NODE_SLAVE 2
#define CLUSTER_NODE_PFAIL 4
#define CLUSTER_NODE_FAIL 8
#define CLUSTER_NODE_MYSELF 16
#define CLUSTER_NODE_HANDSHAKE 32 
#define CLUSTER_NODE_NOADDR 64
#define CLUSTER_NODE_MEET 128
#define CLUSTER_NODE_MIGRATE_TO 256
#define CLUSTER_NODE_NULL_NAME "\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000"


#define nodeIsMaster(n) ((n)->flags & CLUSTER_NODE_MASTER) 
#define nodeIsSlave(n) ((n)->flags & CLUSTER_NODE_SLAVE)
#define nodeInHandshake(n) ((n)->flags & CLUSTER_NODE_HANDSHAKE)
#define nodeHasAddr(n) (!((n)->flags & CLUSTER_NODE_NOADDR))
#define nodeWithoutAddr(n) ((n)->flags & CLUSTER_NODE_NOADDR) 
#define nodeTimedOut(n) ((n)->flags & CLUSTER_NODE_PFAIL)
#define nodeFailed(n) ((n)->flags & CLUSTER_NODE_FAIL)


#define CLUSTER_CANT_FAILOVER_NONE 0
#define CLUSTER_CANT_FAILOVER_DATA_AGE 1
#define CLUSTER_CANT_FAILOVER_WAITING_DELAY 2
#define CLUSTER_CANT_FAILOVER_EXPIRED 3
#define CLUSTER_CANT_FAILOVER_WAITING_VOTES 4
#define CLUSTER_CANT_FAILOVER_RELOG_PERIOD (60 * 5)

typedef struct clusterNodeFailReport {
    struct clusterNode *node;
    mstime_t time;
} clusterNodeFailReport;

typedef struct clusterNode {
    mstime_t ctime;
    char name[CLUSTER_NAMELEN];
    int flags;
    uint64_t configEpoch;
    unsigned char slots[CLUSTER_SLOTS/8];
    int numslots;
    int numslaves;
    struct clusterNode **slaves;
    struct clusterNode *slaveof;
    
    mstime_t ping_sent;
    mstime_t pong_received;
    mstime_t fail_time;
    mstime_t voted_time;
    mstime_t repl_offset_time;
    mstime_t orphaned_time;

    long long repl_offset;
    char ip[NET_IP_STR_LEN];
    int port;
    int cport;
    clusterLink *link;
    list *fail_reports;
} clusterNode;


typedef struct clusterState{
    clusterNode *myself;
    uint64_t currentEpoch;
    int state;
    int size;
    dict *nodes;
    dict *nodes_black_list;
    clusterNode *migrating_slots_to[CLUSTER_SLOTS];
    clusterNode *importing_slots_from[CLUSTER_SLOTS];
    clusterNode *slots[CLUSTER_SLOTS];
    zskiplist *slots_to_keys;
    mstime_t failover_auth_time;
    int failover_auth_count;
    int failover_auth_sent;
    int failover_auth_rank;
    uint64_t failover_auth_epoch;
    int cant_failover_reason;

    mstime_t mf_end;
    clusterNode *mf_slave;
    long long mf_master_offset;

    int mf_can_start;


    uint64_t lastVoteEpoch;
    int todo_before_sleep;
    long long stats_bus_messages_sent;
    long long stats_bus_messages_received;
} clusterState;


#define CLUSTER_TODO_HANDLE_FAILOVER (1<<0)
#define CLUSTER_TODO_UPDATE_STATE (1<<1)
#define CLUSTER_TODO_SAVE_CONFIG (1<<2)
#define CLUSTER_TODO_FSYNC_CONFIG (1<<3)



#define CLUSTERMSG_TYPE_PING 0
#define CLUSTERMSG_TYPE_PONG 1
#define CLUSTERMSG_TYPE_MEET 2
#define CLUSTERMSG_TYPE_FAIL 3
#define CLUSTERMSG_TYPE_PUBLISH 4
#define CLUSTERMSG_TYPE_FAILOVER_AUTH_REQUEST 5
#define CLUSTERMSG_TYPE_FAILOVER_AUTH_ACK 6
#define CLUSTERMSG_TYPE_UPDATE 7
#define CLUSTERMSG_TYPE_MFSTART 8


typedef struct {
    char nodename[CLUSTER_NAMELEN];
    uint32_t ping_sent;
    uint32_t pong_received;
    char ip[NET_IP_STR_LEN];
    uint16_t port;
    uint16_t cport;
    uint16_t flags;
    uint32_t notused1;
} clusterMsgDataGossip;

typedef struct {
    char nodename[CLUSTER_NAMELEN];
} clusterMsgDataFail;


typedef struct {
    uint32_t channel_len;
    uint32_t message_len;
    unsigned char bulk_data[8];
} clusterMsgDataPublish;


typedef struct {
    uint64_t configEpoch;
    char nodename[CLUSTER_NAMELEN];
    unsigned char slots[CLUSTER_SLOTS/8];
} clusterMsgDataUpdate;

union clusterMsgData {
    struct {
        clusterMsgDataGossip gossip[1]; 
    } ping;


    struct {
        clusterMsgDataFail about; 
    } fail;

    struct {
        clusterMsgDataPublish msg; 
    } publish;
    
    struct {
        clusterMsgDataUpdate nodecfg; 
    } update;
}; 


#define CLUSTER_PROTO_VER 1

typedef struct {
    char sig[4];
    uint32_t totlen;
    uint16_t ver;
    uint16_t port;
    uint16_t type;
    uint16_t count;
    uint16_t currentEpoch;
    uint64_t configEpoch;

    uint64_t offset;

    char sender[CLUSTER_NAMELEN];
    unsigned char myslots[CLUSTER_SLOTS/8];
    char slaveof[CLUSTER_NAMELEN];
    char myip[NET_IP_STR_LEN];
    char notused1[34];
    uint16_t cport;
    uint16_t flags;
    unsigned char state;
    unsigned char mflags[3];
    union clusterMsgData data;
} clusterMsg;

#define CLUSTERMSG_MIN_LEN (sizeof(clusterMsg) - sizeof(union clusterMsgData))

#define CLUSTERMSG_FLAG0_PAUSED (1<<0)
#define CLUSTERMSG_FLAG0_FORCEACK (1<<1)


clusterNode *getNodeByQuery(client *c, struct redisCommand *cmd, robj **argv, int argc, int *hashslot, int *ask);
int clusterRedirectBlockedClientIfNeeded(client *c);
void clusterRedirectClient(client *c, clusterNode *n, int hashslot, int error_code);


#endif




  


