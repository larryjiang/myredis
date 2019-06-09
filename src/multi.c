#include "server.h"

void initClientMultiState(client *c){
    c->mstate.commands = NULL;
    c->mstate.count = 0;
};

void freeClientMultiState(client *c){
    int j;

    for(j = 0; j < c->mstate.count; j++){
        int i;
        multiCmd *mc = c->mstate.commands + j;
        for(i = 0; i < mc->argc; i++){
            decrRefCount(mc->argv[i]);
        };
        zfree(mc->argv);
    };

    zfree(c->mstate.commands);
};

void queueMultiCommand(client *c){
    multiCmd *mc;
    int j;

    c->mstate.commands = zrealloc(c->mstate.commands, sizeof(multiCmd) * (c->mstate.count + 1));
    mc = c->mstate.commands + c->mstate.count;
    mc->cmd = c->cmd;
    mc->argc = c->argc;
    mc->argv = zmalloc(sizeof(robj *) * c->argc);
    memcpy(mc->argv, c->argv,sizeof(robj *) * c->argc);
    for(j = 0; j < c->argc; j++){
        incrRefCount(mc->argv[j]);
    };
    c->mstate.count++;
};


void discardTransaction(client *c){
    freeClientMultiState(c);
    initClientMultiState(c);
    c->flags &= ~(CLIENT_MULTI | CLIENT_DIRTY_CAS | CLIENT_DIRTY_EXEC);
    unwatchAllKeys(c);
};

void flagTransaction(client *c){
    if(c->flags & CLIENT_MULTI){
        c->flags |= CLIENT_DIRTY_EXEC;
    };
};


void multiCommand(client *c){
    if(c->flags & CLIENT_MULTI){
        addReplyError(c,"MULTI calls can not be nested");
        return;
    };

    c->flags |= CLIENT_MULTI;
    addReply(c,shared.ok);
};

void discardCommand(client *c){
    if(!(c->flags & CLIENT_MULTI)){
        addReplyError(c,"DISCARD without MULTI");
        return;
    };

    discardTransaction(c);
    addReply(c,shared.ok);
};

void execCommandPropagateMulti(client *c){
    robj *multistring = createStringObject("MULTI",5);
    propagate(server.multiCommand,c->db->id,&multistring, 1, PROPAGATE_AOF|PROPAGATE_REPL);
    decrRefCount(multistring);
};

void execCommand(client *c){
    int j;
    robj **orig_argv;
    int orig_argc;
    struct redisCommand *orig_cmd;
    int must_propagate = 0;

    if(!(c->flags & CLIENT_MULTI)){
        addReplyError(c,"EXEC without MULTI");
        return;
    };

    if(c->flags & (CLIENT_DIRTY_CAS | CLIENT_DIRTY_EXEC)){
        addReply(c,c->flags & CLIENT_DIRTY_EXEC ? shared.execaborterr : shared.nullmultibulk);
        discardTransaction(c);
        goto handle_monitor;
    };

    unwatchAllKeys(c);
    orig_argv = c->argv;
    orig_argc = c->argc;
    orig_cmd = c->cmd;
    addReplyMultiBulkLen(c,c->mstate.count);
    for(j = 0; j < c->mstate.count;j++){
        c->argc = c->mstate.commands[j].argc;
        c->argv = c->mstate.commands[j].argv;
        c->cmd = c->mstate.commands[j].cmd;

        if(!must_propagate && !(c->cmd->flags & CMD_READONLY)){
            execCommandPropagateMulti(c);
            must_propagate = 1;
        };

        call(c,CMD_CALL_FULL);

        c->mstate.commands[j].argc = c->argc;
        c->mstate.commands[j].argv = c->argv;
        c->mstate.commands[j].cmd = c->cmd;
    };

    c->argv = orig_argv;
    c->argc = orig_argc;
    c->cmd = orig_cmd;

    discardTransaction(c);
    if(must_propagate) server.dirty++;

handle_monitor:
    if(listLength(server.monitors) && !server.loading){
        replicationFeedMonitors(c,server.monitors,c->db->id,c->argv,c->argc);
    }
};

typedef struct watchedKey {
    robj *key;
    redisDb *db;
} watchedKey;


void watchForKey(client *c, robj *key){
    list *clients = NULL;
    listIter li;
    listNode *ln;
    watchedKey *wk;

    listRewind(c->watched_keys,&li);
    while((ln = listNext(&li))){
        wk = listNodeValue(ln);
        if(wk->db == c->db && equalStringObjects(key,wk->key)){
            return;
        }
    };

    clients = dictFetchValue(c->db->watched_keys,key);
    if(!clients){
        clients = listCreate();
        dictAdd(c->db->watched_keys,key,clients);
        incrRefCount(key);
    };

    listAddNodeTail(clients,c);
    wk= zmalloc(sizeof(*wk));
    wk->key = key;
    wk->db = c->db;
    incrRefCount(key);
    listAddNodeTail(c->watched_keys,wk);
};

void unwatchAllKeys(client *c){
    listIter li;
    listNode *ln;

    if(listLength(c->watched_keys)) return;
    listRewind(c->watched_keys,&li);

    while((ln=listNext(&li))){
        list *clients;
        watchedKey *wk;

        wk = listNodeValue(ln);
        clients = dictFetchValue(wk->db->watched_keys,wk->key);
        serverAssertWithInfo(c,NULL,clients != NULL);
        listDelNode(clients,listSearchKey(clients,c));
        decrRefCount(wk->key);
        zfree(wk);
    };
};

void touchWatchedKey(redisDb *db, robj *key){
    list *clients;
    listIter li;
    listNode *ln;

    if(dictSize(db->watched_keys) == 0) return;
    clients = dictFetchValue(db->watched_keys,key);
    if(!clients) return;

    listRewind(clients, &li);
    while((ln=listNext(&li))){
        client *c = listNodeValue(ln);
        c->flags |= CLIENT_DIRTY_CAS;
    };
};

void touchWatchedKeysOnFlush(int dbid){
    listIter li1, li2;
    listNode *ln;

    listRewind(server.clients,&li1);
    while((ln=listNext(&li1))){
        client *c = listNodeValue(ln);
        listRewind(c->watched_keys,&li2);
        while((ln=listNext(&li2))){
            watchedKey *wk = listNodeValue(ln);

            if(dbid == -1 || wk->db->id == dbid){
                if(dictFind(wk->db->dict,wk->key->ptr) != NULL){
                    c->flags |= CLIENT_DIRTY_CAS;
                };
            };
        };
    };
};


void watchCommand(client *c){
    int j;
    if(c->flags & CLIENT_MULTI){
        addReplyError(c,"WATCH inside MULTI is not allowed");
        return;
    };

    for(j = 1; j < c->argc; j++){
        watchForKey(c,c->argv[j]);
    };
    addReply(c,shared.ok);
};

void unwatchCommand(client *c){
    unwatchAllKeys(c);
    c->flags &= (~CLIENT_DIRTY_CAS);
    addReply(c,shared.ok);
};




























